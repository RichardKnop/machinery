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

// InitAMQPConnection - AMQPConnection constructor
func InitAMQPConnection(cnf *config.Config) Connectable {
	return AMQPConnection{
		config: cnf,
	}
}

// Open connects to the message queue, opens a channel,
// declares a queue and returns connection, channel
// and queue objects
func (c AMQPConnection) Open() (Connectable, error) {
	var err error

	c.conn, err = amqp.Dial(c.config.BrokerURL)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	err = c.channel.ExchangeDeclare(
		c.config.Exchange,     // name of the exchange
		c.config.ExchangeType, // type
		true,  // durable
		false, // delete when complete
		false, // internal
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Exchange: %s", err)
	}

	c.queue, err = c.channel.QueueDeclare(
		c.config.DefaultQueue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	err = c.channel.QueueBind(
		c.config.DefaultQueue, // name of the queue
		c.config.BindingKey,   // binding key
		c.config.Exchange,     // source exchange
		false,                 // noWait
		nil,                   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Bind: %s", err)
	}

	return c, nil
}

// Close shuts down the connection
func (c AMQPConnection) Close() error {
	err := c.channel.Close()
	if err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	err = c.conn.Close()
	if err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	return nil
}

// WaitForMessages enters a loop and waits for incoming messages
func (c AMQPConnection) WaitForMessages(w *Worker) {
	err := c.channel.Qos(
		3,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	errors.Fail(err, "Failed to set QoS")

	deliveries, err := c.channel.Consume(
		c.queue.Name,  // queue
		w.ConsumerTag, // consumer tag
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	errors.Fail(err, fmt.Sprintf("Queue Consume: %s", err))

	forever := make(chan bool)

	go c.handleDeliveries(deliveries, w)

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func (c AMQPConnection) handleDeliveries(
	deliveries <-chan amqp.Delivery, w *Worker,
) {
	for d := range deliveries {
		log.Printf("Received new message: %s", d.Body)
		d.Ack(false)
		dotCount := bytes.Count(d.Body, []byte("."))
		t := time.Duration(dotCount)
		time.Sleep(t * time.Second)
		w.processMessage(&d)
	}
}

// PublishMessage places a new message on the default queue
func (c AMQPConnection) PublishMessage(body []byte, routingKey string) error {
	if routingKey == "" {
		if c.config.ExchangeType == "direct" {
			routingKey = c.config.BindingKey
		} else {
			routingKey = c.queue.Name
		}
	}
	return c.channel.Publish(
		c.config.Exchange, // exchange
		routingKey,        // routing key
		false,             // mandatory
		false,             // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}
