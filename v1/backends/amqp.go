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
	config *config.Config
}

// NewAMQPBackend creates AMQPBackend instance
func NewAMQPBackend(cnf *config.Config) Backend {
	return Backend(&AMQPBackend{
		config: cnf,
	})
}

// UpdateState updates a task state
func (amqpBackend *AMQPBackend) UpdateState(taskState *TaskState) error {
	conn, channel, _, err := open(taskState.TaskUUID, amqpBackend.config)
	if err != nil {
		return err
	}

	defer close(channel, conn)

	message, err := json.Marshal(taskState)
	if err != nil {
		return fmt.Errorf("JSON Encode Message: %v", err)
	}

	return channel.Publish(
		amqpBackend.config.Exchange, // exchange
		taskState.TaskUUID,          // routing key
		false,                       // mandatory
		false,                       // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         message,
			DeliveryMode: amqp.Transient,
		},
	)
}

// GetState returns the latest task state. It will only return the status once
// as the message will get consumed and removed from the queue.
func (amqpBackend *AMQPBackend) GetState(taskUUID string) (*TaskState, error) {
	taskState := TaskState{}

	conn, channel, queue, err := open(taskUUID, amqpBackend.config)
	if err != nil {
		return &taskState, err
	}

	defer close(channel, conn)

	d, ok, err := channel.Get(
		queue.Name, // queue name
		false,      // multiple
	)
	if err != nil {
		return &taskState, err
	}
	if !ok {
		return &taskState, errors.New("No state ready")
	}

	d.Ack(false)

	if err := json.Unmarshal([]byte(d.Body), &taskState); err != nil {
		log.Printf("Failed to unmarshal task state: %v", d.Body)
		return &taskState, err
	}

	if taskState.IsCompleted() {
		channel.QueueDelete(
			queue.Name, // name
			false,      // ifUnused
			false,      // ifEmpty
			false,      // noWait
		)
	}

	return &taskState, nil
}

// Connects to the message queue, opens a channel, declares a queue
func open(taskUUID string, cnf *config.Config) (*amqp.Connection, *amqp.Channel, amqp.Queue, error) {
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

	err = channel.ExchangeDeclare(
		cnf.Exchange,     // name of the exchange
		cnf.ExchangeType, // type
		true,             // durable
		false,            // delete when complete
		false,            // internal
		false,            // noWait
		nil,              // arguments
	)
	if err != nil {
		return conn, channel, queue, fmt.Errorf("Exchange: %s", err)
	}

	resultsExpireIn := cnf.ResultsExpireIn * 1000
	if resultsExpireIn == 0 {
		// // expire results after 1 hour by default
		resultsExpireIn = 3600 * 1000
	}
	arguments := amqp.Table{
		"x-message-ttl": int32(resultsExpireIn),
	}
	queue, err = channel.QueueDeclare(
		taskUUID, // name
		false,    // durable
		true,     // delete when unused
		false,    // exclusive
		false,    // no-wait
		arguments,
	)
	if err != nil {
		return conn, channel, queue, fmt.Errorf("Queue Declare: %s", err)
	}

	if err := channel.QueueBind(
		queue.Name,   // name of the queue
		taskUUID,     // binding key
		cnf.Exchange, // source exchange
		false,        // noWait
		nil,          // arguments
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
