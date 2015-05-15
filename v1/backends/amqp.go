package backends

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

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
func (amqpBackend AMQPBackend) UpdateState(
	taskUUID, state string, result *TaskResult,
) error {
	openConn, err := amqpBackend.open(taskUUID)
	if err != nil {
		return err
	}

	defer openConn.close()

	taskState := TaskState{
		TaskUUID: taskUUID,
		State:    state,
		Result:   result,
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

// GetState returns the latest task state
// It will only return the status once as the message will get acked!
func (amqpBackend AMQPBackend) GetState(taskUUID string) (*TaskState, error) {
	taskState := TaskState{}

	openConn, err := amqpBackend.open(taskUUID)
	if err != nil {
		return &taskState, err
	}

	defer openConn.close()

	d, ok, err := openConn.channel.Get(taskUUID, false)
	if err != nil {
		return &taskState, err
	}
	if !ok {
		return &taskState, errors.New("No state ready")
	}

	d.Ack(false)

	err = json.Unmarshal([]byte(d.Body), &taskState)
	if err != nil {
		log.Printf("Failed to unmarshal task state: %v", d.Body)
		return &taskState, err
	}

	return &taskState, nil
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
