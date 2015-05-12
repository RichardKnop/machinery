package machinery

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
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

// Returns successive Fibonacci numbers starting from 1
func fibonacci() func() int {
	a, b := 0, 1
	return func() int {
		a, b = b, a+b
		return a
	}
}

// WaitForMessages enters a loop and waits for incoming messages
func (amqpConnection AMQPConnection) WaitForMessages(worker *Worker) error {
	var retryCountDown int
	fibonacci := fibonacci()

	for {
		if retryCountDown > 0 {
			duration, err := time.ParseDuration(
				fmt.Sprintf("%vs", retryCountDown))
			if err != nil {
				return fmt.Errorf("ParseDuration: %s", err)
			}

			log.Printf("Retrying after %v seconds", retryCountDown)
			time.Sleep(duration)
			retryCountDown = fibonacci()
		}

		openConn, err := amqpConnection.open()
		if err != nil {
			return fmt.Errorf("AMQPConnection Open: %s", err)
		}

		defer openConn.close()

		if err := openConn.channel.Qos(
			3,     // prefetch count
			0,     // prefetch size
			false, // global
		); err != nil {
			return fmt.Errorf("Channel Qos: %s", err)
		}

		deliveries, err := openConn.channel.Consume(
			openConn.queue.Name, // queue
			worker.ConsumerTag,  // consumer tag
			false,               // auto-ack
			false,               // exclusive
			false,               // no-local
			false,               // no-wait
			nil,                 // args
		)
		if err != nil {
			return fmt.Errorf("Queue Consume: %s", err)
		}

		forever := make(chan bool)

		go openConn.consume(deliveries, worker)

		log.Print("[*] Waiting for messages. To exit press CTRL+C")
		<-forever

		return nil
	}
}

// PublishMessage places a new message on the default queue
func (amqpConnection AMQPConnection) PublishMessage(
	body []byte, routingKey string,
) error {
	openConn, err := amqpConnection.open()
	if err != nil {
		return fmt.Errorf("AMQPConnection Open: %s", err)
	}

	defer openConn.close()

	return openConn.channel.Publish(
		openConn.config.Exchange,              // exchange
		openConn.adjustRoutingKey(routingKey), // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}

// Connects to the message queue, opens a channel, declares a queue
func (amqpConnection AMQPConnection) open() (*AMQPConnection, error) {
	var err error

	amqpConnection.conn, err = amqp.Dial(amqpConnection.config.BrokerURL)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	amqpConnection.channel, err = amqpConnection.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	err = amqpConnection.channel.ExchangeDeclare(
		amqpConnection.config.Exchange,     // name of the exchange
		amqpConnection.config.ExchangeType, // type
		true,  // durable
		false, // delete when complete
		false, // internal
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Exchange: %s", err)
	}

	amqpConnection.queue, err = amqpConnection.channel.QueueDeclare(
		amqpConnection.config.DefaultQueue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	err = amqpConnection.channel.QueueBind(
		amqpConnection.config.DefaultQueue, // name of the queue
		amqpConnection.config.BindingKey,   // binding key
		amqpConnection.config.Exchange,     // source exchange
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Bind: %s", err)
	}

	return &amqpConnection, nil
}

// Closes the connection
func (amqpConnection AMQPConnection) close() error {
	if err := amqpConnection.channel.Close(); err != nil {
		return fmt.Errorf("Channel Close: %s", err)
	}

	if err := amqpConnection.conn.Close(); err != nil {
		return fmt.Errorf("Connection Close: %s", err)
	}

	return nil
}

// Consumes messages
func (amqpConnection AMQPConnection) consume(
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

// If routing key is empty string:
// a) set it to binding key for direct exchange type
// b) set it to default queue name
func (amqpConnection AMQPConnection) adjustRoutingKey(routingKey string) string {
	if routingKey == "" {
		if amqpConnection.config.ExchangeType == "direct" {
			routingKey = amqpConnection.config.BindingKey
		} else {
			routingKey = amqpConnection.queue.Name
		}
	}
	return routingKey
}
