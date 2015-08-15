package brokers

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"

	"github.com/garyburd/redigo/redis"

	"strings"
)

// RedisBroker represents a Redis broker
type RedisBroker struct {
	config   *config.Config
	stopChan chan int
}

// NewRedisBroker creates new RedisBroker instance
func NewRedisBroker(cnf *config.Config) Broker {
	return Broker(&RedisBroker{
		config: cnf,
	})
}

// StartConsuming enters a loop and waits for incoming messages
func (redisBroker *RedisBroker) StartConsuming(consumerTag string, taskProcessor TaskProcessor) (bool, error) {
	conn, err := openRedisConn(redisBroker.config)
	if err != nil {
		return true, err // retry true
	}

	defer conn.Close()

	redisBroker.stopChan = make(chan int)

	psc := redis.PubSubConn{Conn: conn}
	deliveries := make(chan signatures.TaskSignature)
	errors := make(chan error)

	log.Print("[*] Waiting for messages. To exit press CTRL+C")

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
				log.Print(n)
			}
		}
	}()

	psc.Subscribe(redisBroker.config.DefaultQueue)
	// Unsubscribe from all connections. This will cause the receiving
	// goroutine to exit.
	defer psc.Unsubscribe()

	for {
		select {
		case signature := <-deliveries:
			taskProcessor.Process(&signature)
		case err := <-errors:
			// Unsubscribe from all connections. This will cause the receiving
			// goroutine to exit.
			psc.Unsubscribe()
			return true, err
		case <-redisBroker.stopChan:
			// Unsubscribe from all connections. This will cause the receiving
			// goroutine to exit.
			psc.Unsubscribe()
			return false, nil
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
	conn, err := openRedisConn(redisBroker.config)
	if err != nil {
		return err
	}

	defer conn.Close()

	message, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON Encode Message: %v", err)
	}

	log.Print(message)

	return nil
}

func openRedisConn(cnf *config.Config) (redis.Conn, error) {
	network := "tcp"
	address := strings.Split(cnf.Broker, "redis://")[1]
	return redis.Dial(network, address)
}
