package brokers

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/RichardKnop/machinery/Godeps/_workspace/src/github.com/streadway/amqp"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/RichardKnop/machinery/v1/utils"
)

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

	conn, channel, queue, _, err := amqpBroker.open()
	if err != nil {
		amqpBroker.retryFunc()
		return true, err // retry true
	}

	amqpBroker.retryFunc = utils.RetryClosure()

	defer amqpBroker.close(channel, conn)

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
	conn, channel, _, confirmsChan, err := amqpBroker.open()
	if err != nil {
		return err
	}

	defer amqpBroker.close(channel, conn)

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

// Connects to the message queue, opens a channel, declares a queue
func (amqpBroker *AMQPBroker) open() (*amqp.Connection, *amqp.Channel, amqp.Queue, <-chan amqp.Confirmation, error) {
	var conn *amqp.Connection
	var channel *amqp.Channel
	var queue amqp.Queue
	var err error

	conn, err = amqp.Dial(amqpBroker.config.Broker)
	if err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Dial: %s", err)
	}

	channel, err = conn.Channel()
	if err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Channel: %s", err)
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
		return conn, channel, queue, nil, fmt.Errorf("Exchange: %s", err)
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
		return conn, channel, queue, nil, fmt.Errorf("Queue Declare: %s", err)
	}

	if err := channel.QueueBind(
		queue.Name,                   // name of the queue
		amqpBroker.config.BindingKey, // binding key
		amqpBroker.config.Exchange,   // source exchange
		false, // noWait
		nil,   // arguments
	); err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Queue Bind: %s", err)
	}

	confirmsChan := make(chan amqp.Confirmation, 1)

	// Enable publish confirmations
	if err := channel.Confirm(false); err != nil {
		close(confirmsChan)
		return conn, channel, queue, nil, fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}

	return conn, channel, queue, channel.NotifyPublish(confirmsChan), nil
}

// Closes the connection
func (amqpBroker *AMQPBroker) close(channel *amqp.Channel, conn *amqp.Connection) error {
	if err := channel.Close(); err != nil {
		return fmt.Errorf("Channel Close: %s", err)
	}

	if err := conn.Close(); err != nil {
		return fmt.Errorf("Connection Close: %s", err)
	}

	return nil
}
