package machinery

import (
	"bytes"
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
	return AMQPConnection{config: cnf}
}

// Open connects to the message queue, opens a channel,
// declares a queue and returns connection, channel
// and queue objects
func (c AMQPConnection) Open() Connectable {
	var err error

	c.conn, err = amqp.Dial(c.config.BrokerURL)
	errors.Fail(err, "Failed to connect to RabbitMQ")

	c.channel, err = c.conn.Channel()
	errors.Fail(err, "Failed to open a channel")

	c.queue, err = c.channel.QueueDeclare(
		c.config.DefaultQueue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	errors.Fail(err, "Failed to declare a queue")

	return c
}

// Close shuts down the connection
func (c AMQPConnection) Close() {
	c.conn.Close()
	c.channel.Close()
}

// WaitForMessages enters a loop and waits for incoming messages
func (c AMQPConnection) WaitForMessages(w *Worker) {
	defer c.Close()

	err := c.channel.Qos(
		3,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	errors.Fail(err, "Failed to set QoS")

	deliveries, err := c.channel.Consume(
		c.queue.Name, // queue
		"worker",     // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	errors.Fail(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range deliveries {
			log.Printf("Received new message: %s", d.Body)
			d.Ack(false)
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			time.Sleep(t * time.Second)
			w.processMessage(&d)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

// PublishMessage places a new message on the default queue
func (c AMQPConnection) PublishMessage(body []byte) {
	err := c.channel.Publish(
		"",           // exchange
		c.queue.Name, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	errors.Fail(err, "Failed to publish a message")
}
