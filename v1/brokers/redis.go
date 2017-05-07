package brokers

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/logger"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/RichardKnop/machinery/v1/utils"
	"github.com/garyburd/redigo/redis"
)

var redisDelayedTasksKey = "delayed_tasks"

// RedisBroker represents a Redis broker
type RedisBroker struct {
	host              string
	password          string
	db                int
	pool              *redis.Pool
	stopReceivingChan chan int
	stopDelayedChan   chan int
	receivingWG       sync.WaitGroup
	delayedWG         sync.WaitGroup
	// If set, path to a socket file overrides hostname
	socketPath string
	Broker
}

// NewRedisBroker creates new RedisBroker instance
func NewRedisBroker(cnf *config.Config, host, password, socketPath string, db int) Interface {
	b := &RedisBroker{Broker: Broker{cnf: cnf, retry: true}}
	b.host = host
	b.db = db
	b.password = password
	b.socketPath = socketPath

	return b
}

// StartConsuming enters a loop and waits for incoming messages
func (b *RedisBroker) StartConsuming(consumerTag string, taskProcessor TaskProcessor) (bool, error) {
	b.startConsuming(consumerTag, taskProcessor)

	b.pool = b.newPool()
	defer b.pool.Close()

	// Ping the server to make sure connection is live
	conn := b.pool.Get()
	_, err := conn.Do("PING")
	if err != nil {
		b.retryFunc()
		return b.retry, err
	}
	conn.Close()

	b.retryFunc = utils.RetryClosure()

	// Channels and wait groups used to properly close down goroutines
	b.stopReceivingChan = make(chan int)
	b.stopDelayedChan = make(chan int)
	b.receivingWG.Add(1)
	b.delayedWG.Add(1)

	// Channel to which we will push tasks ready for processing by worker
	deliveries := make(chan []byte)

	// A receivig goroutine keeps popping messages from the queue by BLPOP
	// If the message is valid and can be unmarshaled into a proper structure
	// we send it to the deliveries channel
	go func() {
		defer b.receivingWG.Done()

		logger.Get().Print("[*] Waiting for messages. To exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.stopReceivingChan:
				return
			default:
				task, err := b.nextTask(b.cnf.DefaultQueue)
				if err != nil {
					continue
				}

				deliveries <- task
			}
		}
	}()

	// A goroutine to watch for delayed tasks and push them to deliveries
	// channel for consumption by the worker
	go func() {
		defer b.delayedWG.Done()

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.stopDelayedChan:
				return
			default:
				delayedTask, err := b.nextDelayedTask(redisDelayedTasksKey)
				if err != nil {
					continue
				}

				deliveries <- delayedTask
			}
		}
	}()

	if err := b.consume(deliveries, taskProcessor); err != nil {
		return b.retry, err
	}

	return b.retry, nil
}

// StopConsuming quits the loop
func (b *RedisBroker) StopConsuming() {
	// Stop the receiving goroutine
	b.stopReceiving()

	// Stop the delayed tasks goroutine
	b.stopDelayed()

	b.stopConsuming()
}

// Publish places a new message on the default queue
func (b *RedisBroker) Publish(signature *signatures.TaskSignature) error {
	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON Encode Message: %v", err)
	}

	conn, err := b.open()
	if err != nil {
		return fmt.Errorf("Dial: %s", err)
	}
	defer conn.Close()

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			score := signature.ETA.UnixNano()
			_, err = conn.Do("ZADD", redisDelayedTasksKey, score, msg)
			return err
		}
	}

	_, err = conn.Do("RPUSH", b.cnf.DefaultQueue, msg)
	return err
}

// GetPendingTasks returns a slice of task.Signatures waiting in the queue
func (b *RedisBroker) GetPendingTasks(queue string) ([]*signatures.TaskSignature, error) {
	conn, err := b.open()
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}
	defer conn.Close()

	if queue == "" {
		queue = b.cnf.DefaultQueue
	}
	bytes, err := conn.Do("LRANGE", queue, 0, 10)
	if err != nil {
		return nil, err
	}
	results, err := redis.ByteSlices(bytes, err)
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*signatures.TaskSignature, len(results))
	for i, result := range results {
		sig := new(signatures.TaskSignature)
		if err := json.Unmarshal(result, sig); err != nil {
			return nil, err
		}
		taskSignatures[i] = sig
	}
	return taskSignatures, nil
}

// Consumes messages...
func (b *RedisBroker) consume(deliveries <-chan []byte, taskProcessor TaskProcessor) error {
	maxWorkers := b.cnf.MaxWorkerInstances
	pool := make(chan struct{}, maxWorkers)

	// initialize worker pool with maxWorkers workers
	go func() {
		for i := 0; i < maxWorkers; i++ {
			pool <- struct{}{}
		}
	}()

	errorsChan := make(chan error)
	for {
		select {
		case err := <-errorsChan:
			return err
		case d := <-deliveries:
			if maxWorkers != 0 {
				// get worker from pool (blocks until one is available)
				<-pool
			}
			// Consume the task inside a gotourine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}
				if maxWorkers != 0 {
					// give worker back to pool
					pool <- struct{}{}
				}
			}()
		case <-b.Broker.stopChan:
			return nil
		}
	}
}

// Consume a single message
func (b *RedisBroker) consumeOne(delivery []byte, taskProcessor TaskProcessor) error {
	logger.Get().Printf("Received new message: %s", delivery)

	sig := new(signatures.TaskSignature)
	if err := json.Unmarshal(delivery, sig); err != nil {
		return err
	}

	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(sig.Name) {
		conn := b.pool.Get()
		defer conn.Close()

		conn.Do("RPUSH", b.cnf.DefaultQueue, delivery)
		return nil
	}

	return taskProcessor.Process(sig)
}

// nextTask pops next available task from the default queue
func (b *RedisBroker) nextTask(queue string) (result []byte, err error) {
	conn := b.pool.Get()
	defer conn.Close()

	items, err := redis.ByteSlices(conn.Do("BLPOP", queue, 1))
	if err != nil {
		return []byte{}, err
	}

	// items[0] - the name of the key where an element was popped
	// items[1] - the value of the popped element
	if len(items) != 2 {
		return []byte{}, redis.ErrNil
	}

	result = items[1]

	return result, nil
}

// nextDelayedTask pops a value from the ZSET key using WATCH/MULTI/EXEC commands.
// https://github.com/garyburd/redigo/blob/master/redis/zpop_example_test.go
func (b *RedisBroker) nextDelayedTask(key string) (result []byte, err error) {
	conn := b.pool.Get()
	defer conn.Close()

	defer func() {
		// Return connection to normal state on error.
		if err != nil {
			conn.Do("DISCARD")
		}
	}()

	for {
		if _, err := conn.Do("WATCH", key); err != nil {
			return []byte{}, err
		}

		now := time.Now().UTC().UnixNano()

		// https://redis.io/commands/zrangebyscore
		items, err := redis.ByteSlices(conn.Do(
			"ZRANGEBYSCORE",
			key,
			0,
			now,
			"LIMIT",
			0,
			1,
		))
		if err != nil {
			return []byte{}, err
		}
		if len(items) != 1 {
			return []byte{}, redis.ErrNil
		}

		conn.Send("MULTI")
		conn.Send("ZREM", key, items[0])
		queued, err := conn.Do("EXEC")
		if err != nil {
			return []byte{}, err
		}

		if queued != nil {
			result = items[0]
			break
		}
	}

	return result, nil
}

// Stops the receiving goroutine
func (b *RedisBroker) stopReceiving() {
	b.stopReceivingChan <- 1
	// Waiting for the receiving goroutine to have stopped
	b.receivingWG.Wait()
}

// Stops the delayed tasks goroutine
func (b *RedisBroker) stopDelayed() {
	b.stopDelayedChan <- 1
	// Waiting for the delayed tasks goroutine to have stopped
	b.delayedWG.Wait()
}

// Returns / creates instance of Redis connection
func (b *RedisBroker) open() (redis.Conn, error) {
	if b.socketPath != "" {
		return redis.Dial("unix", b.socketPath, redis.DialPassword(b.password), redis.DialDatabase(b.db))
	}

	// package redis takes care of pwd or db
	return redis.Dial("tcp", b.host, redis.DialPassword(b.password), redis.DialDatabase(b.db))
}

// Returns a new pool of Redis connections
func (b *RedisBroker) newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     8,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			var (
				c    redis.Conn
				err  error
				opts = make([]redis.DialOption, 0)
			)

			if b.password != "" {
				opts = append(opts, redis.DialPassword(b.password))
			}

			if b.socketPath != "" {
				c, err = redis.Dial("unix", b.socketPath, opts...)
			} else {
				c, err = redis.Dial("tcp", b.host, opts...)
			}
			if err != nil {
				return nil, err
			}

			if b.db != 0 {
				_, err = c.Do("SELECT", b.db)
			}

			if err != nil {
				return nil, err
			}
			return c, err
		},
		// PINGs connections that have been idle more than 15 seconds
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			// if time.Since(t) < time.Duration(15 * time.Second) {
			//   return nil
			// }
			_, err := c.Do("PING")
			return err
		},
	}
}
