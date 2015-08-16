package brokers

import (
	"encoding/json"
	"fmt"
	"log"

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
	stopChan  chan int
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
	if redisBroker.retryFunc == nil {
		redisBroker.retryFunc = utils.RetryClosure()
	}

	conn, err := redisBroker.open()
	if err != nil {
		redisBroker.retryFunc()
		return true, fmt.Errorf("Dial: %s", err) // retry true
	}

	redisBroker.retryFunc = utils.RetryClosure()

	defer conn.Close()

	psc := redis.PubSubConn{Conn: conn}
	if err := psc.Subscribe(redisBroker.config.DefaultQueue); err != nil {
		return true, err // retry true
	}

	defer psc.Unsubscribe()
	defer psc.Close()

	redisBroker.stopChan = make(chan int)
	deliveries := make(chan signatures.TaskSignature)
	errors := make(chan error)

	log.Print("[*] Waiting for messages. To exit press CTRL+C")

	// Receving goroutine
	go func() {
		for {
			switch n := psc.Receive().(type) {
			case redis.Message:
				log.Printf("Received new message: %s", n.Data)

				signature := signatures.TaskSignature{}
				if err := json.Unmarshal(n.Data, &signature); err != nil {
					errors <- err
					continue
				}

				deliveries <- signature
			case redis.Subscription:
				if n.Count == 0 {
					return
				}
			case error:
				errors <- n
				continue
			}
		}
	}()

	// Iterate over delivered tasks and process them
	for {
		select {
		case signature := <-deliveries:
			taskProcessor.Process(&signature)
		case err := <-errors:
			return true, err // retry true
		case <-redisBroker.stopChan:
			return false, nil // retry false
		}
	}
}

// StopConsuming quits the loop
func (redisBroker *RedisBroker) StopConsuming() {
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

	_, err = conn.Do("PUBLISH", redisBroker.config.DefaultQueue, message)
	return err
}

// Returns / creates instance of Redis connection
func (redisBroker *RedisBroker) open() (redis.Conn, error) {
	// We need to return a new Redis connection every time as after
	// subscribing to a channel, PUBLISH is not allowed on that connection
	// e.g. ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context
	return redis.Dial("tcp", redisBroker.host)
}
