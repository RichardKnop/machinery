package machinery

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/errors"
	"github.com/streadway/amqp"
)

// AMQPConnection represents an AMQP conenction (e.g. RabbitMQ)
type AMQPConnection struct {
	config  *config.Config
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

// NewAMQPConnection - AMQPConnection constructor
func NewAMQPConnection(cnf *config.Config) Connectable {
	return AMQPConnection{
		config: cnf,
	}
}

// Open connects to the message queue, opens a channel,
// declares a queue and returns connection, channel
// and queue objects
func (connection AMQPConnection) Open() (Connectable, error) {
	var err error

	connection.conn, err = amqp.Dial(connection.config.BrokerURL)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	connection.channel, err = connection.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	err = connection.channel.ExchangeDeclare(
		connection.config.Exchange,     // name of the exchange
		connection.config.ExchangeType, // type
		true,  // durable
		false, // delete when complete
		false, // internal
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Exchange: %s", err)
	}

	connection.queue, err = connection.channel.QueueDeclare(
		connection.config.DefaultQueue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	err = connection.channel.QueueBind(
		connection.config.DefaultQueue, // name of the queue
		connection.config.BindingKey,   // binding key
		connection.config.Exchange,     // source exchange
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Bind: %s", err)
	}

	return connection, nil
}

// Close shuts down the connection
func (connection AMQPConnection) Close() error {
	err := connection.channel.Close()
	if err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	err = connection.conn.Close()
	if err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	return nil
}

// WaitForMessages enters a loop and waits for incoming messages
func (connection AMQPConnection) WaitForMessages(worker *Worker) {
	err := connection.channel.Qos(
		3,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	errors.Fail(err, "Failed to set QoS")

	deliveries, err := connection.channel.Consume(
		connection.queue.Name, // queue
		worker.ConsumerTag,    // consumer tag
		false,                 // auto-ack
		false,                 // exclusive
		false,                 // no-local
		false,                 // no-wait
		nil,                   // args
	)
	errors.Fail(err, fmt.Sprintf("Queue Consume: %s", err))

	forever := make(chan bool)

	go connection.handleDeliveries(deliveries, worker)

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func (connection AMQPConnection) handleDeliveries(
	deliveries <-chan amqp.Delivery, worker *Worker,
) {
	for d := range deliveries {
		log.Printf("Received new message: %s", d.Body)
		d.Ack(false)
		dotCount := bytes.Count(d.Body, []byte("."))
		t := time.Duration(dotCount)
		time.Sleep(t * time.Second)
		worker.processMessage(&d)
	}
}

// PublishMessage places a new message on the default queue
func (connection AMQPConnection) PublishMessage(
	body []byte, routingKey string,
) error {
	if routingKey == "" {
		if connection.config.ExchangeType == "direct" {
			routingKey = connection.config.BindingKey
		} else {
			routingKey = connection.queue.Name
		}
	}
	return connection.channel.Publish(
		connection.config.Exchange, // exchange
		routingKey,                 // routing key
		false,                      // mandatory
		false,                      // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}
