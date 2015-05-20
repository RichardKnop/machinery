package brokers

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/RichardKnop/machinery/v1/utils"
	"github.com/streadway/amqp"
)

// AMQPBroker represents an AMQP broker
type AMQPBroker struct {
	config  *config.Config
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
	quit    chan int
}

// NewAMQPBroker creates new AMQPConnection instance
func NewAMQPBroker(cnf *config.Config, quit chan int) Broker {
	return Broker(&AMQPBroker{
		config: cnf,
		quit:   quit,
	})
}

// StartConsuming enters a loop and waits for incoming messages
func (amqpBroker *AMQPBroker) StartConsuming(consumerTag string, taskProcessor TaskProcessor) error {
	retryIn := 0
	fibonacci := utils.Fibonacci()

	retryReset := func() {
		// Next time there is a connection problem,
		// we want to start a new Fibonacci retry sequence
		fibonacci = utils.Fibonacci()
		retryIn = 0
	}

	retry := func() {
		if retryIn > 0 {
			// This means there was a problem opening a connection
			// We will try reconnecting in 1, 1, 2, 3, 5, 8... (Fibonacci)
			durationString := fmt.Sprintf("%vs", retryIn)
			duration, _ := time.ParseDuration(durationString)

			log.Printf("Retrying in %v seconds", retryIn)
			time.Sleep(duration)
		}
		retryIn = fibonacci()
	}

	for {
		retry()

		conn, channel, queue, err := open(amqpBroker.config)
		if err != nil {
			log.Print(err)
			continue
		}

		retryReset()

		defer close(channel, conn)

		if err := channel.Qos(
			3,     // prefetch count
			0,     // prefetch size
			false, // global
		); err != nil {
			return fmt.Errorf("Channel Qos: %s", err)
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
			return fmt.Errorf("Queue Consume: %s", err)
		}

		log.Print("[*] Waiting for messages. To exit press CTRL+C")

		amqpBroker.consume(deliveries, taskProcessor)

		log.Print("Quitting the worker")

		break
	}

	return nil
}

// StopConsuming quits the loop
func (amqpBroker *AMQPBroker) StopConsuming() {
	amqpBroker.quit <- 1
}

// Publish places a new message on the default queue
func (amqpBroker *AMQPBroker) Publish(signature *signatures.TaskSignature) error {
	conn, channel, _, err := open(amqpBroker.config)
	if err != nil {
		return err
	}

	defer close(channel, conn)

	message, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON Encode Message: %v", err)
	}

	signature.AdjustRoutingKey(
		amqpBroker.config.ExchangeType,
		amqpBroker.config.BindingKey,
		amqpBroker.config.DefaultQueue,
	)
	return channel.Publish(
		amqpBroker.config.Exchange, // exchange
		signature.RoutingKey,       // routing key
		false,                      // mandatory
		false,                      // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         message,
			DeliveryMode: amqp.Persistent,
		},
	)
}

// Consumes messages
func (amqpBroker *AMQPBroker) consume(deliveries <-chan amqp.Delivery, taskProcessor TaskProcessor) {
	consumeOne := func(d amqp.Delivery) {
		log.Printf("Received new message: %s", d.Body)
		d.Ack(false)

		signature := signatures.TaskSignature{}
		if err := json.Unmarshal([]byte(d.Body), &signature); err != nil {
			log.Printf("Failed to unmarshal task singnature: %v", string(d.Body))
			return
		}

		taskProcessor.Process(&signature)
	}

	for {
		select {
		case d := <-deliveries:
			consumeOne(d)
		case <-amqpBroker.quit:
			return
		}
	}
}

// Connects to the message queue, opens a channel, declares a queue
func open(cnf *config.Config) (*amqp.Connection, *amqp.Channel, amqp.Queue, error) {
	var conn *amqp.Connection
	var channel *amqp.Channel
	var queue amqp.Queue
	var err error

	conn, err = amqp.Dial(cnf.Broker)
	if err != nil {
		return conn, channel, queue, fmt.Errorf("Dial: %s", err)
	}

	channel, err = conn.Channel()
	if err != nil {
		return conn, channel, queue, fmt.Errorf("Channel: %s", err)
	}

	if err := channel.ExchangeDeclare(
		cnf.Exchange,     // name of the exchange
		cnf.ExchangeType, // type
		true,             // durable
		false,            // delete when complete
		false,            // internal
		false,            // noWait
		nil,              // arguments
	); err != nil {
		return conn, channel, queue, fmt.Errorf("Exchange: %s", err)
	}

	queue, err = channel.QueueDeclare(
		cnf.DefaultQueue, // name
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		return conn, channel, queue, fmt.Errorf("Queue Declare: %s", err)
	}

	if err := channel.QueueBind(
		queue.Name,     // name of the queue
		cnf.BindingKey, // binding key
		cnf.Exchange,   // source exchange
		false,          // noWait
		nil,            // arguments
	); err != nil {
		return conn, channel, queue, fmt.Errorf("Queue Bind: %s", err)
	}

	return conn, channel, queue, nil
}

// Closes the connection
func close(channel *amqp.Channel, conn *amqp.Connection) error {
	if err := channel.Close(); err != nil {
		return fmt.Errorf("Channel Close: %s", err)
	}

	if err := conn.Close(); err != nil {
		return fmt.Errorf("Connection Close: %s", err)
	}

	return nil
}
