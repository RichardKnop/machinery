package brokers

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/streadway/amqp"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/RichardKnop/machinery/v1/utils"
)

var once sync.Once
var conn *amqp.Connection

// AMQPBroker represents an AMQP broker
type AMQPBroker struct {
	config    *config.Config
	retryFunc func()
	stopChan  chan int
}

// NewAMQPBroker creates new AMQPBroker instance
func NewAMQPBroker(cnf *config.Config) Broker {
	return Broker(&AMQPBroker{
		config: cnf,
	})
}

// StartConsuming enters a loop and waits for incoming messages
func (amqpBroker *AMQPBroker) StartConsuming(consumerTag string, taskProcessor TaskProcessor) (bool, error) {
	if amqpBroker.retryFunc == nil {
		amqpBroker.retryFunc = utils.RetryClosure()
	}

	_, channel, queue, _, err := amqpBroker.open()
	defer channel.Close()
	if err != nil {
		amqpBroker.retryFunc()
		return true, err // retry true
	}

	amqpBroker.retryFunc = utils.RetryClosure()

	amqpBroker.stopChan = make(chan int)

	if err := channel.Qos(
		3,     // prefetch count
		0,     // prefetch size
		false, // global
	); err != nil {
		return false, fmt.Errorf("Channel Qos: %s", err)
	}

	deliveries, err := channel.Consume(
		queue.Name,  // queue
		consumerTag, // consumer tag
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		return false, fmt.Errorf("Queue Consume: %s", err)
	}

	log.Print("[*] Waiting for messages. To exit press CTRL+C")

	if err := amqpBroker.consume(deliveries, taskProcessor); err != nil {
		return true, err // retry true
	}

	return false, nil
}

// StopConsuming quits the loop
func (amqpBroker *AMQPBroker) StopConsuming() {
	// Notifying the stop channel stops consuming of messages
	amqpBroker.stopChan <- 1
}

// Publish places a new message on the default queue
func (amqpBroker *AMQPBroker) Publish(signature *signatures.TaskSignature) error {
	_, channel, _, confirmsChan, err := amqpBroker.open()
	defer channel.Close()
	if err != nil {
		return err
	}

	message, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON Encode Message: %v", err)
	}

	signature.AdjustRoutingKey(
		amqpBroker.config.ExchangeType,
		amqpBroker.config.BindingKey,
		amqpBroker.config.DefaultQueue,
	)
	if err := channel.Publish(
		amqpBroker.config.Exchange, // exchange
		signature.RoutingKey,       // routing key
		false,                      // mandatory
		false,                      // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         message,
			DeliveryMode: amqp.Persistent,
		},
	); err != nil {
		return err
	}

	confirmed := <-confirmsChan

	if confirmed.Ack {
		return nil
	}

	return fmt.Errorf("Failed delivery of delivery tag: %v", confirmed.DeliveryTag)
}

// Consume a single message
func (amqpBroker *AMQPBroker) consumeOne(d amqp.Delivery, taskProcessor TaskProcessor, errorsChan chan error) {
	if len(d.Body) == 0 {
		d.Nack(false, false)                                   // multiple, requeue
		errorsChan <- errors.New("Received an empty message.") // RabbitMQ down?
		return
	}

	log.Printf("Received new message: %s", d.Body)

	signature := signatures.TaskSignature{}
	if err := json.Unmarshal(d.Body, &signature); err != nil {
		d.Nack(false, false) // multiple, requeue
		errorsChan <- err
		return
	}

	d.Ack(false) // multiple

	if err := taskProcessor.Process(&signature); err != nil {
		errorsChan <- err
	}
}

// Consumes messages...
func (amqpBroker *AMQPBroker) consume(deliveries <-chan amqp.Delivery, taskProcessor TaskProcessor) error {
	errorsChan := make(chan error)
	for {
		select {
		case err := <-errorsChan:
			return err
		case d := <-deliveries:
			// Consume the task inside a gotourine so multiple tasks
			// can be processed concurrently
			go func() {
				amqpBroker.consumeOne(d, taskProcessor, errorsChan)
			}()
		case <-amqpBroker.stopChan:
			return nil
		}
	}
}

// Connects to the message queue
func (amqpBroker *AMQPBroker) connect() {

	var err error
	fmt.Println("connecting...")
	conn, err = amqp.Dial(amqpBroker.config.Broker)
	if err != nil {
		fmt.Printf("Dial: %s\n", err)
	}

}

// Connects to the message queue, opens a channel, declares a queue
func (amqpBroker *AMQPBroker) open() (*amqp.Connection, *amqp.Channel, amqp.Queue, <-chan amqp.Confirmation, error) {
	var err error
	var channel *amqp.Channel
	var queue amqp.Queue
	if conn == nil {
		once.Do(amqpBroker.connect)
	}

	if conn == nil {
		return conn, channel, queue, nil, fmt.Errorf("Can't connect to the server")
	}

	channel, err = conn.Channel()
	if err != nil {
		fmt.Printf("Channel: %s\n", err)
	}
	if err := channel.ExchangeDeclare(
		amqpBroker.config.Exchange,     // name of the exchange
		amqpBroker.config.ExchangeType, // type
		true,  // durable
		false, // delete when complete
		false, // internal
		false, // noWait
		nil,   // arguments
	); err != nil {
		fmt.Printf("Exchange: %s\n", err)
	}

	queue, err = channel.QueueDeclare(
		amqpBroker.config.DefaultQueue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		fmt.Printf("Queue Declare: %s\n", err)
	}

	if err := channel.QueueBind(
		queue.Name,                   // name of the queue
		amqpBroker.config.BindingKey, // binding key
		amqpBroker.config.Exchange,   // source exchange
		false, // noWait
		nil,   // arguments
	); err != nil {
		fmt.Printf("Queue Bind: %s\n", err)
	}

	confirmsChan := make(chan amqp.Confirmation, 1)
	// Enable publish confirmations
	if err := channel.Confirm(false); err != nil {
		close(confirmsChan)
		fmt.Printf("Channel could not be put into confirm mode: %s\n", err)
	}

	return conn, channel, queue, channel.NotifyPublish(confirmsChan), nil
}
