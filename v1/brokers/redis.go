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
	config    *config.Config
	host      string
	retryFunc func()
	quitChan  chan int
	stopChan  chan int
	wg        sync.WaitGroup
}

// NewRedisBroker creates new RedisBroker instance
func NewRedisBroker(cnf *config.Config, host string) Broker {
	return Broker(&RedisBroker{
		config: cnf,
		host:   host,
	})
}

// StartConsuming enters a loop and waits for incoming messages
func (redisBroker *RedisBroker) StartConsuming(consumerTag string, taskProcessor TaskProcessor) (bool, error) {
	redisBroker.stopChan = make(chan int)
	redisBroker.quitChan = make(chan int)
	redisBroker.wg.Add(1)
	errorsChan := make(chan error)
	if redisBroker.retryFunc == nil {
		redisBroker.retryFunc = utils.RetryClosure()
	}

	go redisBroker.consume(errorsChan, taskProcessor)

	for {
		select {
		case err := <-errorsChan:
			return true, err // retry true
		case <-redisBroker.stopChan:
			return false, nil // retry false
		}
	}
}

// StopConsuming quits the loop
func (redisBroker *RedisBroker) StopConsuming() {
	// Notifying the quit channel stops receiving goroutine
	redisBroker.quitChan <- 1
	// Wait for the receiving goroutine to have stopped
	redisBroker.wg.Wait()
	// Notifying the quit channel stops consuming of messages
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

// Consumes messages
func (redisBroker *RedisBroker) consume(errorsChan chan error, taskProcessor TaskProcessor) {
	defer redisBroker.wg.Done()

	for {
		conn, err := redisBroker.open()
		if err != nil {
			redisBroker.retryFunc()
			errorsChan <- err
			return
		}

		redisBroker.retryFunc = utils.RetryClosure()

		defer conn.Close()

		log.Print("[*] Waiting for messages. To exit press CTRL+C")

		select {
		case <-redisBroker.quitChan:
			return
		default:
			// Return value of BLPOP is an array. For example:
			// redis> RPUSH list1 a b c
			// (integer) 3
			// redis> BLPOP list1 list2 0
			// 1) "list1"
			// 2) "a"
			multiBulk, err := redis.MultiBulk(conn.Do("BLPOP", redisBroker.config.DefaultQueue, "0"))
			if err != nil {
				errorsChan <- err
				return
			}

			item, err := redis.Bytes(multiBulk[1], nil)
			if err != nil {
				errorsChan <- err
				return
			}

			log.Printf("Received new message: %s", item)

			signature := signatures.TaskSignature{}
			if err := json.Unmarshal(item, &signature); err != nil {
				errorsChan <- err
				return
			}

			if err := taskProcessor.Process(&signature); err != nil {
				errorsChan <- err
				return
			}
		}
	}
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
