package brokers

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/streadway/amqp"
)

// AMQPBroker represents an AMQP broker
type AMQPBroker struct {
	config  *config.Config
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

// NewAMQPBroker creates new AMQPConnection instance
func NewAMQPBroker(cnf *config.Config) Broker {
	return AMQPBroker{
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

// Consume enters a loop and waits for incoming messages
func (amqpBroker AMQPBroker) Consume(
	consumerTag string, mp MessageProcessor,
) error {
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

		openConn, err := amqpBroker.open()
		if err != nil {
			return fmt.Errorf("AMQPBroker Open: %s", err)
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
			consumerTag,         // consumer tag
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

		go openConn.consume(deliveries, mp)

		log.Print("[*] Waiting for messages. To exit press CTRL+C")
		<-forever

		return nil
	}
}

// Publish places a new message on the default queue
func (amqpBroker AMQPBroker) Publish(
	body []byte, routingKey string,
) error {
	openConn, err := amqpBroker.open()
	if err != nil {
		return err
	}

	defer openConn.close()

	return openConn.channel.Publish(
		openConn.config.Exchange,              // exchange
		openConn.adjustRoutingKey(routingKey), // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		},
	)
}

// Connects to the message queue, opens a channel, declares a queue
func (amqpBroker AMQPBroker) open() (*AMQPBroker, error) {
	var err error

	amqpBroker.conn, err = amqp.Dial(amqpBroker.config.Broker)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	amqpBroker.channel, err = amqpBroker.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	err = amqpBroker.channel.ExchangeDeclare(
		amqpBroker.config.Exchange,     // name of the exchange
		amqpBroker.config.ExchangeType, // type
		true,  // durable
		false, // delete when complete
		false, // internal
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Exchange: %s", err)
	}

	amqpBroker.queue, err = amqpBroker.channel.QueueDeclare(
		amqpBroker.config.DefaultQueue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	err = amqpBroker.channel.QueueBind(
		amqpBroker.queue.Name,        // name of the queue
		amqpBroker.config.BindingKey, // binding key
		amqpBroker.config.Exchange,   // source exchange
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Bind: %s", err)
	}

	return &amqpBroker, nil
}

// Closes the connection
func (amqpBroker AMQPBroker) close() error {
	if err := amqpBroker.channel.Close(); err != nil {
		return fmt.Errorf("Channel Close: %s", err)
	}

	if err := amqpBroker.conn.Close(); err != nil {
		return fmt.Errorf("Connection Close: %s", err)
	}

	return nil
}

// Consumes messages
func (amqpBroker AMQPBroker) consume(
	deliveries <-chan amqp.Delivery, mp MessageProcessor,
) {
	for d := range deliveries {
		log.Printf("Received new message: %s", d.Body)
		d.Ack(false)
		dotCount := bytes.Count(d.Body, []byte("."))
		t := time.Duration(dotCount)
		time.Sleep(t * time.Second)
		mp.ProcessMessage(&d)
	}
}

// If routing key is empty string:
// a) set it to binding key for direct exchange type
// b) set it to default queue name
func (amqpBroker AMQPBroker) adjustRoutingKey(routingKey string) string {
	if routingKey != "" {
		return routingKey
	}

	if amqpBroker.config.ExchangeType == "direct" {
		return amqpBroker.config.BindingKey
	}
	return amqpBroker.queue.Name
}
