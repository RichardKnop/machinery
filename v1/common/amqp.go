package common

import (
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/streadway/amqp"
)

// AMQPConnector ...
type AMQPConnector struct {
	conn *amqp.Connection
	rmu  *sync.RWMutex
}

func NewAMQPConnector() *AMQPConnector {
	return &AMQPConnector{
		rmu: &sync.RWMutex{},
	}
}

// Connect opens a connection to RabbitMQ, declares an exchange, opens a channel,
// declares and binds the queue and enables publish notifications
func (ac *AMQPConnector) Connect(url string, tlsConfig *tls.Config, exchange, exchangeType, queueName string, queueDurable, queueDelete bool, queueBindingKey string, exchangeDeclareArgs, queueDeclareArgs, queueBindingArgs amqp.Table) (*amqp.Connection, *amqp.Channel, amqp.Queue, <-chan amqp.Confirmation, <-chan *amqp.Error, error) {
	return ac.connect(false, url, tlsConfig, exchange, exchangeType, queueName, queueDurable, queueDelete, queueBindingKey, exchangeDeclareArgs, queueDeclareArgs, queueBindingArgs)
}

func (ac *AMQPConnector) ConnectKeepAlive(url string, tlsConfig *tls.Config, exchange, exchangeType, queueName string, queueDurable, queueDelete bool, queueBindingKey string, exchangeDeclareArgs, queueDeclareArgs, queueBindingArgs amqp.Table) (*amqp.Connection, *amqp.Channel, amqp.Queue, <-chan amqp.Confirmation, <-chan *amqp.Error, error) {
	return ac.connect(true, url, tlsConfig, exchange, exchangeType, queueName, queueDurable, queueDelete, queueBindingKey, exchangeDeclareArgs, queueDeclareArgs, queueBindingArgs)
}

func (ac *AMQPConnector) connect(keepAlive bool, url string, tlsConfig *tls.Config, exchange, exchangeType, queueName string, queueDurable, queueDelete bool, queueBindingKey string, exchangeDeclareArgs, queueDeclareArgs, queueBindingArgs amqp.Table) (*amqp.Connection, *amqp.Channel, amqp.Queue, <-chan amqp.Confirmation, <-chan *amqp.Error, error) {
	// Connect to server
	conn, channel, err := ac.open(url, tlsConfig, keepAlive)
	if err != nil {
		return nil, nil, amqp.Queue{}, nil, nil, err
	}

	if exchange != "" {
		// Declare an exchange
		if err = channel.ExchangeDeclare(
			exchange,            // name of the exchange
			exchangeType,        // type
			true,                // durable
			false,               // delete when complete
			false,               // internal
			false,               // noWait
			exchangeDeclareArgs, // arguments
		); err != nil {
			return conn, channel, amqp.Queue{}, nil, nil, fmt.Errorf("Exchange declare error: %s", err)
		}
	}

	var queue amqp.Queue
	if queueName != "" {
		// Declare a queue
		queue, err = channel.QueueDeclare(
			queueName,        // name
			queueDurable,     // durable
			queueDelete,      // delete when unused
			false,            // exclusive
			false,            // no-wait
			queueDeclareArgs, // arguments
		)
		if err != nil {
			return conn, channel, amqp.Queue{}, nil, nil, fmt.Errorf("Queue declare error: %s", err)
		}

		// Bind the queue
		if err = channel.QueueBind(
			queue.Name,       // name of the queue
			queueBindingKey,  // binding key
			exchange,         // source exchange
			false,            // noWait
			queueBindingArgs, // arguments
		); err != nil {
			return conn, channel, queue, nil, nil, fmt.Errorf("Queue bind error: %s", err)
		}
	}

	// Enable publish confirmations
	if err = channel.Confirm(false); err != nil {
		return conn, channel, queue, nil, nil, fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}

	return conn, channel, queue, channel.NotifyPublish(make(chan amqp.Confirmation, 1)), conn.NotifyClose(make(chan *amqp.Error, 1)), nil
}

// DeleteQueue deletes a queue by name
func (ac *AMQPConnector) DeleteQueue(channel *amqp.Channel, queueName string) error {
	// First return value is number of messages removed
	_, err := channel.QueueDelete(
		queueName, // name
		false,     // ifUnused
		false,     // ifEmpty
		false,     // noWait
	)

	return err
}

// InspectQueue provides information about a specific queue
func (*AMQPConnector) InspectQueue(channel *amqp.Channel, queueName string) (*amqp.Queue, error) {
	queueState, err := channel.QueueInspect(queueName)
	if err != nil {
		return nil, fmt.Errorf("Queue inspect error: %s", err)
	}

	return &queueState, nil
}

// Open new RabbitMQ connection
func (ac *AMQPConnector) Open(url string, tlsConfig *tls.Config) (*amqp.Connection, *amqp.Channel, error) {
	return ac.open(url, tlsConfig, false)
}

func (ac *AMQPConnector) open(url string, tlsConfig *tls.Config, keepAlive bool) (*amqp.Connection, *amqp.Channel, error) {
	var (
		conn *amqp.Connection
		err  error
	)

	ac.rmu.RLock()
	createNew := ac.conn == nil
	ac.rmu.RUnlock()

	if !keepAlive || createNew {
		conn, err = ac.createNewConn(url, tlsConfig)
	} else {
		conn = ac.conn
	}
	if err != nil {
		return nil, nil, err
	}

	// Open a channel
	channel, err := conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("Open channel error: %s", err)
	}

	return conn, channel, nil
}

func (ac *AMQPConnector) createNewConn(url string, tlsConfig *tls.Config) (*amqp.Connection, error) {
	// Connect
	// From amqp docs: DialTLS will use the provided tls.Config when it encounters an amqps:// scheme
	// and will dial a plain connection when it encounters an amqp:// scheme.
	conn, err := amqp.DialTLS(url, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("Dial error: %s", err)
	}
	ac.setConn(conn)
	ac.waitForConnClose()
	return conn, err
}

func (ac *AMQPConnector) setConn(conn *amqp.Connection) {
	ac.rmu.Lock()
	defer ac.rmu.Unlock()
	ac.conn = conn
}

func (ac *AMQPConnector) waitForConnClose() {
	recv := make(chan *amqp.Error)
	go func() {
		<-recv
		ac.setConn(nil)
	}()
	ac.conn.NotifyClose(recv)
}

// Close connection
func (ac *AMQPConnector) Close(channel *amqp.Channel, conn *amqp.Connection) error {
	if channel != nil {
		if err := channel.Close(); err != nil {
			return fmt.Errorf("Close channel error: %s", err)
		}
	}

	if conn != nil {
		if err := conn.Close(); err != nil {
			return fmt.Errorf("Close connection error: %s", err)
		}
	}

	return nil
}
