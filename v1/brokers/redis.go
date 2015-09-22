package brokers

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/RichardKnop/machinery/Godeps/_workspace/src/github.com/garyburd/redigo/redis"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/RichardKnop/machinery/v1/utils"
)

// RedisBroker represents a Redis broker
type RedisBroker struct {
	config              *config.Config
	registeredTaskNames []string
	host                string
	pool                *redis.Pool
	retryFunc           func()
	stopChan            chan int
	stopReceivingChan   chan int
	errorsChan          chan error
	wg                  sync.WaitGroup
}

// NewRedisBroker creates new RedisBroker instance
func NewRedisBroker(cnf *config.Config, host string) Broker {
	return Broker(&RedisBroker{
		config: cnf,
		host:   host,
	})
}

// SetRegisteredTaskNames sets registered task names
func (redisBroker *RedisBroker) SetRegisteredTaskNames(names []string) {
	redisBroker.registeredTaskNames = names
}

// IsTaskRegistered returns true if the task is registered with this broker
func (redisBroker *RedisBroker) IsTaskRegistered(name string) bool {
	for _, registeredTaskName := range redisBroker.registeredTaskNames {
		if registeredTaskName == name {
			return true
		}
	}
	return false
}

// StartConsuming enters a loop and waits for incoming messages
func (redisBroker *RedisBroker) StartConsuming(consumerTag string, taskProcessor TaskProcessor) (bool, error) {
	if redisBroker.retryFunc == nil {
		redisBroker.retryFunc = utils.RetryClosure()
	}

	redisBroker.pool = redisBroker.newPool()
	defer redisBroker.pool.Close()

	_, err := redisBroker.pool.Get().Do("PING")
	if err != nil {
		redisBroker.retryFunc()
		return true, err // retry true
	}

	redisBroker.retryFunc = utils.RetryClosure()
	redisBroker.stopChan = make(chan int)
	redisBroker.stopReceivingChan = make(chan int)
	redisBroker.errorsChan = make(chan error)
	deliveries := make(chan []byte)

	redisBroker.wg.Add(1)

	go func() {
		defer redisBroker.wg.Done()

		log.Print("[*] Waiting for messages. To exit press CTRL+C")

		conn := redisBroker.pool.Get()

		for {
			select {
			// A way to stop this goroutine from redisBroker.StopConsuming
			case <-redisBroker.stopReceivingChan:
				return
			default:
				itemBytes, err := conn.Do("LPOP", redisBroker.config.DefaultQueue)
				if err != nil {
					redisBroker.errorsChan <- err
					return
				}
				// Unline BLPOP, LPOP is non blocking so nil means we can keep iterating
				if itemBytes == nil {
					continue
				}

				item, err := redis.Bytes(itemBytes, nil)
				if err != nil {
					redisBroker.errorsChan <- err
					return
				}

				signature := signatures.TaskSignature{}
				if err := json.Unmarshal(item, &signature); err != nil {
					redisBroker.errorsChan <- err
					return
				}

				// If the task is not registered, we requeue it,
				// there might be different workers for processing specific tasks
				if !redisBroker.IsTaskRegistered(signature.Name) {
					_, err := conn.Do("RPUSH", redisBroker.config.DefaultQueue, item)

					if err != nil {
						redisBroker.errorsChan <- err
						return
					}

					continue
				}

				deliveries <- item
			}
		}
	}()

	if err := redisBroker.consume(deliveries, taskProcessor); err != nil {
		return true, err // retry true
	}

	return false, nil
}

// StopConsuming quits the loop
func (redisBroker *RedisBroker) StopConsuming() {
	redisBroker.stopReceiving()
	// Notifying the stop channel stops consuming of messages
	redisBroker.stopChan <- 1
}

// Publish places a new message on the default queue
func (redisBroker *RedisBroker) Publish(signature *signatures.TaskSignature) error {
	conn, err := redisBroker.open()
	if err != nil {
		fmt.Errorf("Dial: %s", err)
	}
	defer conn.Close()

	message, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON Encode Message: %v", err)
	}

	_, err = conn.Do("RPUSH", redisBroker.config.DefaultQueue, message)
	return err
}

// Consume a single message
func (redisBroker *RedisBroker) consumeOne(item []byte, taskProcessor TaskProcessor) {
	log.Printf("Received new message: %s", item)

	signature := signatures.TaskSignature{}
	if err := json.Unmarshal(item, &signature); err != nil {
		redisBroker.errorsChan <- err
		return
	}

	if err := taskProcessor.Process(&signature); err != nil {
		redisBroker.errorsChan <- err
	}
}

// Consumes messages...
func (redisBroker *RedisBroker) consume(deliveries <-chan []byte, taskProcessor TaskProcessor) error {
	for {
		select {
		case err := <-redisBroker.errorsChan:
			return err
		case d := <-deliveries:
			// Consume the task inside a gotourine so multiple tasks
			// can be processed concurrently
			go func() {
				redisBroker.consumeOne(d, taskProcessor)
			}()
		case <-redisBroker.stopChan:
			return nil
		}
	}
}

// Stops the receving goroutine
func (redisBroker *RedisBroker) stopReceiving() {
	redisBroker.stopReceivingChan <- 1
	// Waiting for the receiving goroutine to have stopped
	redisBroker.wg.Wait()
}

// Returns / creates instance of Redis connection
func (redisBroker *RedisBroker) open() (redis.Conn, error) {
	return redis.Dial("tcp", redisBroker.host)
}

// Returns a new pool of Redis connections
func (redisBroker *RedisBroker) newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", redisBroker.host)
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
