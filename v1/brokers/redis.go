package brokers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/garyburd/redigo/redis"
	"gopkg.in/redsync.v1"
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
	processingWG      sync.WaitGroup // use wait group to make sure task processing completes on interrupt signal
	receivingWG       sync.WaitGroup
	delayedWG         sync.WaitGroup
	// If set, path to a socket file overrides hostname
	socketPath string
	redsync    *redsync.Redsync
	Broker
	common.RedisConnector
}

// NewRedisBroker creates new RedisBroker instance
func NewRedisBroker(cnf *config.Config, host, password, socketPath string, db int) Interface {
	b := &RedisBroker{Broker: New(cnf)}
	b.host = host
	b.db = db
	b.password = password
	b.socketPath = socketPath

	return b
}

// StartConsuming enters a loop and waits for incoming messages
func (b *RedisBroker) StartConsuming(consumerTag string, concurrency int, taskProcessor TaskProcessor) (bool, error) {
	b.startConsuming(consumerTag, taskProcessor)

	b.pool = nil
	conn := b.open()
	defer conn.Close()
	defer b.pool.Close()

	// Ping the server to make sure connection is live
	_, err := conn.Do("PING")
	if err != nil {
		b.retryFunc(b.retryStopChan)
		return b.retry, err
	}

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

		log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

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
				task, err := b.nextDelayedTask(redisDelayedTasksKey)
				if err != nil {
					continue
				}

				signature := new(tasks.Signature)
				decoder := json.NewDecoder(bytes.NewReader(task))
				decoder.UseNumber()
				if err := decoder.Decode(signature); err != nil {
					log.ERROR.Print(NewErrCouldNotUnmarshaTaskSignature(task, err))
				}

				if err := b.Publish(signature); err != nil {
					log.ERROR.Print(err)
				}
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor); err != nil {
		return b.retry, err
	}

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	return b.retry, nil
}

// StopConsuming quits the loop
func (b *RedisBroker) StopConsuming() {
	// Stop the receiving goroutine
	b.stopReceivingChan <- 1
	// Waiting for the receiving goroutine to have stopped
	b.receivingWG.Wait()

	// Stop the delayed tasks goroutine
	b.stopDelayedChan <- 1
	// Waiting for the delayed tasks goroutine to have stopped
	b.delayedWG.Wait()

	b.stopConsuming()

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()
}

// Publish places a new message on the default queue
func (b *RedisBroker) Publish(signature *tasks.Signature) error {
	// Adjust routing key (this decides which queue the message will be published to)
	AdjustRoutingKey(b, signature)

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	conn := b.open()
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

	_, err = conn.Do("RPUSH", signature.RoutingKey, msg)
	return err
}

// GetPendingTasks returns a slice of task signatures waiting in the queue
func (b *RedisBroker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	conn := b.open()
	defer conn.Close()

	if queue == "" {
		queue = b.cnf.DefaultQueue
	}
	dataBytes, err := conn.Do("LRANGE", queue, 0, 10)
	if err != nil {
		return nil, err
	}
	results, err := redis.ByteSlices(dataBytes, err)
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(bytes.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *RedisBroker) consume(deliveries <-chan []byte, concurrency int, taskProcessor TaskProcessor) error {
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	errorsChan := make(chan error, concurrency*2)

	for {
		select {
		case err := <-errorsChan:
			return err
		case d := <-deliveries:
			if concurrency > 0 {
				// get worker from pool (blocks until one is available)
				<-pool
			}

			b.processingWG.Add(1)

			// Consume the task inside a gotourine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give worker back to pool
					pool <- struct{}{}
				}
			}()
		case <-b.Broker.stopChan:
			return nil
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *RedisBroker) consumeOne(delivery []byte, taskProcessor TaskProcessor) error {
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return NewErrCouldNotUnmarshaTaskSignature(delivery, err)
	}

	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		conn := b.open()
		defer conn.Close()

		conn.Do("RPUSH", b.cnf.DefaultQueue, delivery)
		return nil
	}

	log.INFO.Printf("Received new message: %s", delivery)

	return taskProcessor.Process(signature)
}

// nextTask pops next available task from the default queue
func (b *RedisBroker) nextTask(queue string) (result []byte, err error) {
	conn := b.open()
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
	conn := b.open()
	defer conn.Close()

	defer func() {
		// Return connection to normal state on error.
		// https://redis.io/commands/discard
		if err != nil {
			conn.Do("DISCARD")
		}
	}()

	var (
		items [][]byte
		reply interface{}
	)

	for {
		// Space out queries to ZSET to 20ms intervals so we don't bombard redis
		// server with relentless ZRANGEBYSCOREs
		<-time.After(20 * time.Millisecond)

		if _, err = conn.Do("WATCH", key); err != nil {
			return
		}

		now := time.Now().UTC().UnixNano()

		// https://redis.io/commands/zrangebyscore
		items, err = redis.ByteSlices(conn.Do(
			"ZRANGEBYSCORE",
			key,
			0,
			now,
			"LIMIT",
			0,
			1,
		))
		if err != nil {
			return
		}
		if len(items) != 1 {
			err = redis.ErrNil
			return
		}

		conn.Send("MULTI")
		conn.Send("ZREM", key, items[0])
		reply, err = conn.Do("EXEC")
		if err != nil {
			return
		}

		if reply != nil {
			result = items[0]
			break
		}
	}

	return
}

// open returns or creates instance of Redis connection
func (b *RedisBroker) open() redis.Conn {
	if b.pool == nil {
		b.pool = b.NewPool(b.socketPath, b.host, b.password, b.db)
	}
	if b.redsync == nil {
		var pools = []redsync.Pool{b.pool}
		b.redsync = redsync.New(pools)
	}
	return b.pool.Get()
}
