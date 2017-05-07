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

// RedisBroker represents a Redis broker
type RedisBroker struct {
	host              string
	password          string
	db                int
	pool              *redis.Pool
	stopReceivingChan chan int
	wg                sync.WaitGroup
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

	_, err := b.pool.Get().Do("PING")
	if err != nil {
		b.retryFunc()
		return b.retry, err
	}

	b.retryFunc = utils.RetryClosure()

	b.stopReceivingChan = make(chan int)
	b.wg.Add(1)
	deliveries := make(chan []byte)

	// A receivig goroutine keeps popping messages from the queue by BLPOP
	// If the message is valid and can be unmarshaled into a proper structure
	// we send it to the deliveries channel
	go func() {
		defer b.wg.Done()

		logger.Get().Print("[*] Waiting for messages. To exit press CTRL+C")

		conn := b.pool.Get()

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.stopReceivingChan:
				return
			default:
				// Log error if BLPOP fails
				itemBytes, err := conn.Do("BLPOP", b.cnf.DefaultQueue, "1")
				if err != nil {
					logger.Get().Print(err)
					continue
				}

				// Ignore empty bytes
				if itemBytes == nil {
					continue
				}

				// Log error if we can't convert the message to byte slices
				items, err := redis.ByteSlices(itemBytes, nil)
				if err != nil {
					logger.Get().Print(err)
					continue
				}

				// items[0] - queue name (key), items[1] - value
				if len(items) != 2 {
					logger.Get().Println("Got unexpected amount of byte slices, ignoring")
					continue
				}

				deliveries <- items[1]
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

	b.stopConsuming()
}

// Publish places a new message on the default queue
func (b *RedisBroker) Publish(signature *signatures.TaskSignature) error {
	message, err := json.Marshal(signature)
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
			// delayMs := int64(signature.ETA.Sub(now) / time.Millisecond)
			// TODO - delay task
		}
	}

	_, err = conn.Do("RPUSH", b.cnf.DefaultQueue, message)
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

	var taskSignatures []*signatures.TaskSignature
	for _, result := range results {
		var taskSignature signatures.TaskSignature
		if err := json.Unmarshal(result, &taskSignature); err != nil {
			return nil, err
		}
		taskSignatures = append(taskSignatures, &taskSignature)
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

	signature := new(signatures.TaskSignature)
	if err := json.Unmarshal(delivery, signature); err != nil {
		return err
	}

	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		b.pool.Get().Do("RPUSH", b.cnf.DefaultQueue, delivery)
		return nil
	}

	return taskProcessor.Process(signature)
}

// Stops the receiving goroutine
func (b *RedisBroker) stopReceiving() {
	b.stopReceivingChan <- 1
	// Waiting for the receiving goroutine to have stopped
	b.wg.Wait()
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
		MaxIdle:     3,
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
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
