package backends

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
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

// SetStatePending - sets task state to PENDING
func (amqpBackend *AMQPBackend) SetStatePending(signature *signatures.TaskSignature) error {
	taskState := NewPendingTaskState(signature)

	if err := amqpBackend.updateState(taskState); err != nil {
		return err
	}

	if signature.GroupUUID != "" {
		return amqpBackend.updateStateGroup(signature.GroupUUID, taskState)
	}

	return nil
}

// SetStateReceived - sets task state to RECEIVED
func (amqpBackend *AMQPBackend) SetStateReceived(signature *signatures.TaskSignature) error {
	taskState := NewReceivedTaskState(signature)

	if err := amqpBackend.updateState(taskState); err != nil {
		return err
	}

	if signature.GroupUUID != "" {
		return amqpBackend.updateStateGroup(signature.GroupUUID, taskState)
	}

	return nil
}

// SetStateStarted - sets task state to STARTED
func (amqpBackend *AMQPBackend) SetStateStarted(signature *signatures.TaskSignature) error {
	taskState := NewStartedTaskState(signature)

	if err := amqpBackend.updateState(taskState); err != nil {
		return err
	}

	if signature.GroupUUID != "" {
		return amqpBackend.updateStateGroup(signature.GroupUUID, taskState)
	}

	return nil
}

// SetStateSuccess - sets task state to SUCCESS
func (amqpBackend *AMQPBackend) SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) error {
	taskState := NewSuccessTaskState(signature, result)

	if err := amqpBackend.updateState(taskState); err != nil {
		return err
	}

	if signature.GroupUUID != "" {
		return amqpBackend.updateStateGroup(signature.GroupUUID, taskState)
	}

	return nil
}

// SetStateFailure - sets task state to FAILURE
func (amqpBackend *AMQPBackend) SetStateFailure(signature *signatures.TaskSignature, err string) error {
	taskState := NewFailureTaskState(signature, err)

	if err := amqpBackend.updateState(taskState); err != nil {
		return err
	}

	if signature.GroupUUID != "" {
		return amqpBackend.updateStateGroup(signature.GroupUUID, taskState)
	}

	return nil
}

// GetState returns the latest task state. It will only return the status once
// as the message will get consumed and removed from the queue.
func (amqpBackend *AMQPBackend) GetState(signature *signatures.TaskSignature) (*TaskState, error) {
	taskState := TaskState{}

	conn, channel, queue, err := open(signature.UUID, amqpBackend.config)
	if err != nil {
		return nil, err
	}

	defer close(channel, conn)

	d, ok, err := channel.Get(
		queue.Name, // queue name
		false,      // multiple
	)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("No state ready")
	}

	d.Ack(false)

	if err := json.Unmarshal([]byte(d.Body), &taskState); err != nil {
		log.Printf("Failed to unmarshal task state: %v", string(d.Body))
		log.Print(err)
		return nil, err
	}

	return &taskState, nil
}

// PurgeState - deletes stored task state
func (amqpBackend *AMQPBackend) PurgeState(signature *signatures.TaskSignature) error {
	conn, channel, queue, err := open(signature.UUID, amqpBackend.config)
	if err != nil {
		return err
	}

	defer close(channel, conn)

	// First return value is number of messages removed
	_, err = channel.QueueDelete(
		queue.Name, // name
		false,      // ifUnused
		false,      // ifEmpty
		false,      // noWait
	)

	return err
}

// Updates a task state
func (amqpBackend *AMQPBackend) updateState(taskState *TaskState) error {
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

// Updates a task state group
func (amqpBackend *AMQPBackend) updateStateGroup(groupUUID string, taskState *TaskState) error {
	conn, channel, queue, err := open(groupUUID, amqpBackend.config)
	if err != nil {
		return err
	}

	defer close(channel, conn)

	var taskStateGroup TaskStateGroup

	d, ok, err := channel.Get(
		queue.Name, // queue name
		false,      // multiple
	)
	if err != nil {
		return err
	}
	if !ok {
		taskStateGroup = TaskStateGroup{
			GroupUUID: groupUUID,
			States:    make(map[string]TaskState),
		}
	} else {
		d.Ack(false)

		taskStateGroup = TaskStateGroup{}

		if err := json.Unmarshal([]byte(d.Body), &taskStateGroup); err != nil {
			log.Printf("Failed to unmarshal task state group: %v", string(d.Body))
			log.Print(err)
			return err
		}
	}

	taskStateGroup.States[taskState.TaskUUID] = *taskState

	message, err := json.Marshal(taskStateGroup)
	if err != nil {
		return fmt.Errorf("JSON Encode Message: %v", err)
	}

	return channel.Publish(
		amqpBackend.config.Exchange, // exchange
		groupUUID,                   // routing key
		false,                       // mandatory
		false,                       // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         message,
			DeliveryMode: amqp.Transient,
		},
	)
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
