package backends

// NOTE: Using AMQP as a result backend is quite tricky since every time we
// read a message from the queue keeping task states, the message is removed
// from the queue. This leads to problems with keeping a reliable state of a
// group of tasks since concurrent processes updating the group state cause
// race conditions and inconsistent state.
//
// This is avoided by a "clever" hack. A special queue identified by a group
// UUID is created and we store serialised TaskState objects of successfully
// completed tasks. By inspecting the queue we can then say:
// 1) If all group tasks finished (number of unacked messages = group task count)
// 2) If all group tasks finished AND succeeded (by consuming the queue)
//
// It is important to consume the queue exclusively to avoid race conditions.

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

// InitGroup - saves UUIDs of all tasks in a group
func (amqpBackend *AMQPBackend) InitGroup(groupUUID string, taskUUIDs []string) error {
	return nil
}

// GroupCompleted - returns true if all tasks in a group finished
// NOTE: Given AMQP limitation this will only return true if all finished
// tasks were successful as we do not keep track of completed failed tasks
func (amqpBackend *AMQPBackend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	conn, channel, _, _, err := amqpBackend.open(groupUUID)
	if err != nil {
		return false, err
	}

	defer amqpBackend.close(channel, conn)

	queueState, err := channel.QueueInspect(groupUUID)
	if err != nil {
		return false, fmt.Errorf("Queue Inspect: %v", err)
	}

	return queueState.Messages == groupTaskCount, nil
}

// GroupTaskStates - returns states of all tasks in the group
func (amqpBackend *AMQPBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*TaskState, error) {
	taskStates := make([]*TaskState, groupTaskCount)

	conn, channel, queue, _, err := amqpBackend.open(groupUUID)
	if err != nil {
		return taskStates, err
	}

	defer amqpBackend.close(channel, conn)

	queueState, err := channel.QueueInspect(groupUUID)
	if err != nil {
		return taskStates, fmt.Errorf("Queue Inspect: %v", err)
	}

	if queueState.Messages != groupTaskCount {
		return taskStates, fmt.Errorf("Already consumed: %v", err)
	}

	deliveries, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer tag
		false,      // auto-ack
		true,       // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		return taskStates, fmt.Errorf("Queue Consume: %s", err)
	}

	for i := 0; i < groupTaskCount; i++ {
		d := <-deliveries

		taskState := &TaskState{}

		if err := json.Unmarshal([]byte(d.Body), &taskState); err != nil {
			d.Nack(false, false) // multiple, requeue
			return taskStates, err
		}

		d.Ack(false) // multiple

		taskStates[i] = taskState
	}

	return taskStates, nil
}

// SetStatePending - sets task state to PENDING
func (amqpBackend *AMQPBackend) SetStatePending(signature *signatures.TaskSignature) error {
	taskState := NewPendingTaskState(signature)
	return amqpBackend.updateState(taskState)
}

// SetStateReceived - sets task state to RECEIVED
func (amqpBackend *AMQPBackend) SetStateReceived(signature *signatures.TaskSignature) error {
	taskState := NewReceivedTaskState(signature)
	return amqpBackend.updateState(taskState)
}

// SetStateStarted - sets task state to STARTED
func (amqpBackend *AMQPBackend) SetStateStarted(signature *signatures.TaskSignature) error {
	taskState := NewStartedTaskState(signature)
	return amqpBackend.updateState(taskState)
}

// SetStateSuccess - sets task state to SUCCESS
func (amqpBackend *AMQPBackend) SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) error {
	taskState := NewSuccessTaskState(signature, result)

	if err := amqpBackend.updateState(taskState); err != nil {
		return err
	}

	if signature.GroupUUID == "" {
		return nil
	}

	return amqpBackend.markTaskSuccess(signature, taskState)
}

// SetStateFailure - sets task state to FAILURE
func (amqpBackend *AMQPBackend) SetStateFailure(signature *signatures.TaskSignature, err string) error {
	taskState := NewFailureTaskState(signature, err)
	return amqpBackend.updateState(taskState)
}

// GetState - returns the latest task state. It will only return the status once
// as the message will get consumed and removed from the queue.
func (amqpBackend *AMQPBackend) GetState(taskUUID string) (*TaskState, error) {
	taskState := TaskState{}

	conn, channel, queue, _, err := amqpBackend.open(taskUUID)
	if err != nil {
		return nil, err
	}

	defer amqpBackend.close(channel, conn)

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
func (amqpBackend *AMQPBackend) PurgeState(taskUUID string) error {
	return amqpBackend.deleteQueue(taskUUID)
}

// PurgeGroupMeta - deletes stored group meta data
func (amqpBackend *AMQPBackend) PurgeGroupMeta(groupUUID string) error {
	return amqpBackend.deleteQueue(groupUUID)
}

// Updates a task state
func (amqpBackend *AMQPBackend) updateState(taskState *TaskState) error {
	conn, channel, _, confirmsChan, err := amqpBackend.open(taskState.TaskUUID)
	if err != nil {
		return err
	}

	defer amqpBackend.close(channel, conn)

	message, err := json.Marshal(taskState)
	if err != nil {
		return fmt.Errorf("JSON Encode Message: %v", err)
	}

	if err := channel.Publish(
		amqpBackend.config.Exchange, // exchange
		taskState.TaskUUID,          // routing key
		false,                       // mandatory
		false,                       // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         message,
			DeliveryMode: amqp.Persistent, // Persistent // Transient
		},
	); err != nil {
		return err
	}

	confirmed := <-confirmsChan

	if confirmed.Ack {
		return nil
	}

	return fmt.Errorf("Failed delivery of delivery tag: %v", confirmed.DeliveryTag)
}

// Returns expiration time
func (amqpBackend *AMQPBackend) getExpiresIn() int {
	resultsExpireIn := amqpBackend.config.ResultsExpireIn * 1000
	if resultsExpireIn == 0 {
		// // expire results after 1 hour by default
		resultsExpireIn = 3600 * 1000
	}
	return resultsExpireIn
}

// Deletes a queue
func (amqpBackend *AMQPBackend) deleteQueue(queueName string) error {
	conn, channel, queue, _, err := amqpBackend.open(queueName)
	if err != nil {
		return err
	}

	defer amqpBackend.close(channel, conn)

	// First return value is number of messages removed
	_, err = channel.QueueDelete(
		queue.Name, // name
		false,      // ifUnused
		false,      // ifEmpty
		false,      // noWait
	)

	return err
}

// Marks task as successful in a group queue
// This is important for amqpBackend.GroupCompleted/GroupSuccessful methods
func (amqpBackend *AMQPBackend) markTaskSuccess(signature *signatures.TaskSignature, taskState *TaskState) error {
	if signature.GroupUUID == "" || signature.GroupTaskCount == 0 {
		return nil
	}

	conn, channel, _, confirmsChan, err := amqpBackend.open(signature.GroupUUID)
	if err != nil {
		return err
	}

	defer amqpBackend.close(channel, conn)

	message, err := json.Marshal(taskState)
	if err != nil {
		return fmt.Errorf("JSON Encode Message: %v", err)
	}

	if err := channel.Publish(
		amqpBackend.config.Exchange, // exchange
		signature.GroupUUID,         // routing key
		false,                       // mandatory
		false,                       // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         message,
			DeliveryMode: amqp.Persistent, // Persistent // Transient
		},
	); err != nil {
		return err
	}

	confirmed := <-confirmsChan

	if !confirmed.Ack {
		return fmt.Errorf("Failed delivery of delivery tag: %v", confirmed.DeliveryTag)
	}

	return nil
}

// Connects to the message queue, opens a channel, declares a queue
func (amqpBackend *AMQPBackend) open(taskUUID string) (*amqp.Connection, *amqp.Channel, amqp.Queue, <-chan amqp.Confirmation, error) {
	var (
		conn    *amqp.Connection
		channel *amqp.Channel
		queue   amqp.Queue
		err     error
	)

	// Connect
	conn, err = amqp.Dial(amqpBackend.config.ResultBackend)
	if err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Dial: %s", err)
	}

	// Open a channel
	channel, err = conn.Channel()
	if err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Channel: %s", err)
	}

	// Declare an exchange
	err = channel.ExchangeDeclare(
		amqpBackend.config.Exchange,     // name of the exchange
		amqpBackend.config.ExchangeType, // type
		true,  // durable
		false, // delete when complete
		false, // internal
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Exchange Declare: %s", err)
	}

	// Declare a queue
	arguments := amqp.Table{
		"x-message-ttl": int32(amqpBackend.getExpiresIn()),
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
		return conn, channel, queue, nil, fmt.Errorf("Queue Declare: %s", err)
	}

	// Bind the queue
	if err := channel.QueueBind(
		queue.Name,                  // name of the queue
		taskUUID,                    // binding key
		amqpBackend.config.Exchange, // source exchange
		false, // noWait
		nil,   // arguments
	); err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Queue Bind: %s", err)
	}

	// Enable publish confirmations
	if err := channel.Confirm(false); err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}

	return conn, channel, queue, channel.NotifyPublish(make(chan amqp.Confirmation, 1)), nil
}

// Closes the connection
func (amqpBackend *AMQPBackend) close(channel *amqp.Channel, conn *amqp.Connection) error {
	if err := channel.Close(); err != nil {
		return fmt.Errorf("Channel Close: %s", err)
	}

	if err := conn.Close(); err != nil {
		return fmt.Errorf("Connection Close: %s", err)
	}

	return nil
}
