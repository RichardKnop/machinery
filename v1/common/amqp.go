package common

import (
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/streadway/amqp"
)

// AMQPConnector ...
type AMQPConnector struct {
	connManager *amqpConnManager
}

func NewAMQPConnector(url string, tlsConfig *tls.Config) *AMQPConnector {
	return &AMQPConnector{
		connManager: newAMQPConnManager(url, tlsConfig),
	}
}

// Connect opens a connection to RabbitMQ, declares an exchange, opens a channel,
// declares and binds the queue and enables publish notifications
func (ac *AMQPConnector) Connect(exchange, exchangeType, queueName string, queueDurable, queueDelete bool, queueBindingKey string, exchangeDeclareArgs, queueDeclareArgs, queueBindingArgs amqp.Table) (*amqp.Channel, amqp.Queue, <-chan amqp.Confirmation, <-chan *amqp.Error, error) {
	// Connect to server
	conn, channel, err := ac.GetConn()
	if err != nil {
		return nil, amqp.Queue{}, nil, nil, err
	}

	if exchange != "" {
		// Declare an exchange
		err := channel.ExchangeDeclare(
			exchange,            // name of the exchange
			exchangeType,        // type
			true,                // durable
			false,               // delete when complete
			false,               // internal
			false,               // noWait
			exchangeDeclareArgs, // arguments
		)
		if err != nil {
			channel.Close()
			return nil, amqp.Queue{}, nil, nil, fmt.Errorf("Exchange declare error: %s", err)
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
			channel.Close()
			return nil, amqp.Queue{}, nil, nil, fmt.Errorf("Queue declare error: %s", err)
		}

		// Bind the queue
		err = channel.QueueBind(
			queue.Name,       // name of the queue
			queueBindingKey,  // binding key
			exchange,         // source exchange
			false,            // noWait
			queueBindingArgs, // arguments
		)
		if err != nil {
			channel.Close()
			return nil, amqp.Queue{}, nil, nil, fmt.Errorf("Queue bind error: %s", err)
		}
	}

	// Enable publish confirmations
	if err := channel.Confirm(false); err != nil {
		channel.Close()
		return nil, amqp.Queue{}, nil, nil, fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}

	return channel, queue, channel.NotifyPublish(make(chan amqp.Confirmation, 1)), conn.NotifyClose(make(chan *amqp.Error, 1)), nil
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

func (ac *AMQPConnector) GetConn() (*amqp.Connection, *amqp.Channel, error) {
	return ac.connManager.getConn()
}

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

type amqpConnManager struct {
	url       string
	tlsConfig *tls.Config
	conn      *amqp.Connection
	connID    int
	closeChan chan *amqp.Error
	mu        *sync.Mutex
}

func newAMQPConnManager(url string, tlsConfig *tls.Config) *amqpConnManager {
	closeChan := make(chan *amqp.Error, 1)
	closeChan <- amqp.ErrClosed // get ready for the first getConn
	return &amqpConnManager{
		url:       url,
		tlsConfig: tlsConfig,
		closeChan: closeChan,
		mu:        &sync.Mutex{},
	}
}

func (m *amqpConnManager) getConn() (*amqp.Connection, *amqp.Channel, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case <-m.closeChan:
		err := m.makeConn()
		if err != nil {
			return nil, nil, err
		}
	default:
	}

	channel, err := m.conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("Open channel error: %s", err)
	}

	return m.conn, channel, nil
}

func (m *amqpConnManager) makeConn() error {
	if m.conn != nil {
		m.conn.Close() // this is most likely useless, but just to be sure
	}
	conn, err := amqp.DialTLS(m.url, m.tlsConfig)
	if err != nil {
		return err
	}
	m.closeChan = conn.NotifyClose(make(chan *amqp.Error, 1))
	m.conn = conn
	return nil
}
