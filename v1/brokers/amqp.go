package brokers

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/RichardKnop/machinery/v1/utils"
	"github.com/streadway/amqp"
)

// AMQPBroker represents an AMQP broker
type AMQPBroker struct {
	config              *config.Config
	registeredTaskNames []string
	retry               bool
	retryFunc           func()
	stopChan            chan int
}

// NewAMQPBroker creates new AMQPBroker instance
func NewAMQPBroker(cnf *config.Config) Broker {
	return Broker(&AMQPBroker{
		config: cnf,
		retry:  true,
	})
}

// SetRegisteredTaskNames sets registered task names
func (amqpBroker *AMQPBroker) SetRegisteredTaskNames(names []string) {
	amqpBroker.registeredTaskNames = names
}

// IsTaskRegistered returns true if the task is registered with this broker
func (amqpBroker *AMQPBroker) IsTaskRegistered(name string) bool {
	for _, registeredTaskName := range amqpBroker.registeredTaskNames {
		if registeredTaskName == name {
			return true
		}
	}
	return false
}

// StartConsuming enters a loop and waits for incoming messages
func (amqpBroker *AMQPBroker) StartConsuming(consumerTag string, taskProcessor TaskProcessor) (bool, error) {
	if amqpBroker.retryFunc == nil {
		amqpBroker.retryFunc = utils.RetryClosure()
	}

	conn, channel, queue, _, err := amqpBroker.open()
	if err != nil {
		amqpBroker.retryFunc()
		return amqpBroker.retry, err // retry true
	}

	defer amqpBroker.close(channel, conn)

	amqpBroker.retryFunc = utils.RetryClosure()

	amqpBroker.stopChan = make(chan int)

	if err := channel.Qos(
		3,     // prefetch count
		0,     // prefetch size
		false, // global
	); err != nil {
		return amqpBroker.retry, fmt.Errorf("Channel Qos: %s", err)
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
		return amqpBroker.retry, fmt.Errorf("Queue Consume: %s", err)
	}

	log.Print("[*] Waiting for messages. To exit press CTRL+C")

	if err := amqpBroker.consume(deliveries, taskProcessor); err != nil {
		return amqpBroker.retry, err // retry true
	}

	return amqpBroker.retry, nil
}

// StopConsuming quits the loop
func (amqpBroker *AMQPBroker) StopConsuming() {
	// Do not retry from now on
	amqpBroker.retry = false
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
			Headers:      amqp.Table(signature.Headers),
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

// GetPendingTasks returns a slice of task.Signatures waiting in the queue
func (amqpBroker *AMQPBroker) GetPendingTasks(queue string) ([]*signatures.TaskSignature, error) {
	return nil, errors.New("Not implemented")
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

	// If the task is not registered, we nack it and requeue,
	// there might be different workers for processing specific tasks
	if !amqpBroker.IsTaskRegistered(signature.Name) {
		d.Nack(false, true) // multiple, requeue
		return
	}

	if err := taskProcessor.Process(&signature); err != nil {
		errorsChan <- err
	}

	d.Ack(false) // multiple
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
	var (
		conn    *amqp.Connection
		channel *amqp.Channel
		queue   amqp.Queue
		err     error
	)

	// Connect
	// From amqp docs: DialTLS will use the provided tls.Config when it encounters an amqps:// scheme
	// and will dial a plain connection when it encounters an amqp:// scheme.
	conn, err = amqp.DialTLS(amqpBroker.config.Broker, amqpBroker.config.TLSConfig)

	if err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Dial: %s", err)
	}

	// Open a channel
	channel, err = conn.Channel()
	if err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Channel: %s", err)
	}

	// Declare an exchange
	if err := channel.ExchangeDeclare(
		amqpBroker.config.Exchange,     // name of the exchange
		amqpBroker.config.ExchangeType, // type
		true,  // durable
		false, // delete when complete
		false, // internal
		false, // noWait
		nil,   // arguments
	); err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Exchange Declare: %s", err)
	}

	// Declare a queue
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

	// Bind the queue
	if err := channel.QueueBind(
		queue.Name,                   // name of the queue
		amqpBroker.config.BindingKey, // binding key
		amqpBroker.config.Exchange,   // source exchange
		false, // noWait
		amqp.Table(amqpBroker.config.QueueBindingArguments), // arguments
	); err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Queue Bind: %s", err)
	}

	// Enable publish confirmations
	if err := channel.Confirm(false); err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}

	return conn, channel, queue, channel.NotifyPublish(make(chan amqp.Confirmation, 1)), nil
}

// Closes the connection
func (amqpBroker *AMQPBroker) close(channel *amqp.Channel, conn *amqp.Connection) error {
	if channel != nil {
		if err := channel.Close(); err != nil {
			return fmt.Errorf("Channel Close: %s", err)
		}
	}

	if conn != nil {
		if err := conn.Close(); err != nil {
			return fmt.Errorf("Connection Close: %s", err)
		}
	}

	return nil
}
