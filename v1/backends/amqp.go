package backends

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/streadway/amqp"
)

// AMQPBackend represents an AMQP result backend
type AMQPBackend struct {
	config  *config.Config
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

// NewAMQPBackend creates AMQPBackend instance
func NewAMQPBackend(cnf *config.Config) Backend {
	return AMQPBackend{
		config: cnf,
	}
}

// UpdateState updates a task state
func (amqpBackend AMQPBackend) UpdateState(taskUUID, state string) error {
	openConn, err := amqpBackend.open(taskUUID)
	if err != nil {
		return err
	}

	defer openConn.close()

	taskState := TaskState{
		TaskUUID: taskUUID,
		State:    state,
	}

	message, err := json.Marshal(taskState)

	if err != nil {
		return fmt.Errorf("JSON Encode Message: %v", err)
	}

	return openConn.channel.Publish(
		openConn.config.Exchange, // exchange
		taskUUID,                 // routing key
		false,                    // mandatory
		false,                    // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         message,
			DeliveryMode: amqp.Transient,
		},
	)
}

// GetState returns the current state of a task
func (amqpBackend AMQPBackend) GetState(taskUUID string) (string, error) {
	openConn, err := amqpBackend.open(taskUUID)
	if err != nil {
		return "", err
	}

	defer openConn.close()

	d, ok, err := openConn.channel.Get(taskUUID, false)

	if err != nil {
		return "", err
	}

	if !ok {
		return "", errors.New("No state ready")
	}

	d.Ack(false)

	state := TaskState{}
	json.Unmarshal([]byte(d.Body), &state)

	return state.State, nil
}

// Connects to the message queue, opens a channel, declares a queue
func (amqpBackend AMQPBackend) open(taskUUID string) (*AMQPBackend, error) {
	var err error

	amqpBackend.conn, err = amqp.Dial(amqpBackend.config.Broker)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	amqpBackend.channel, err = amqpBackend.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	err = amqpBackend.channel.ExchangeDeclare(
		amqpBackend.config.Exchange,     // name of the exchange
		amqpBackend.config.ExchangeType, // type
		true,  // durable
		false, // delete when complete
		false, // internal
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Exchange: %s", err)
	}

	amqpBackend.queue, err = amqpBackend.channel.QueueDeclare(
		taskUUID, // name
		false,    // durable
		true,     // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
		//amqp.Table{"x-message-ttl": int32(100)}, // expire in 100 ms
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	err = amqpBackend.channel.QueueBind(
		amqpBackend.queue.Name,      // name of the queue
		taskUUID,                    // binding key
		amqpBackend.config.Exchange, // source exchange
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Bind: %s", err)
	}

	return &amqpBackend, nil
}

// Closes the connection
func (amqpBackend AMQPBackend) close() error {
	if err := amqpBackend.channel.Close(); err != nil {
		return fmt.Errorf("Channel Close: %s", err)
	}

	if err := amqpBackend.conn.Close(); err != nil {
		return fmt.Errorf("Connection Close: %s", err)
	}

	return nil
}