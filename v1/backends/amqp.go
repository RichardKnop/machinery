package backends

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/RichardKnop/machinery/Godeps/_workspace/src/github.com/streadway/amqp"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
)

// AMQPBackend represents an AMQP result backend
type AMQPBackend struct {
	config          *config.Config
	resultsExpireIn int32
}

// NewAMQPBackend creates AMQPBackend instance
func NewAMQPBackend(cnf *config.Config) Backend {
	resultsExpireIn := cnf.ResultsExpireIn * 1000
	if resultsExpireIn == 0 {
		// // expire results after 1 hour by default
		resultsExpireIn = 3600 * 1000
	}
	return Backend(&AMQPBackend{
		config:          cnf,
		resultsExpireIn: int32(resultsExpireIn),
	})
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
func (amqpBackend *AMQPBackend) SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) (*TaskStateGroup, error) {
	taskState := NewSuccessTaskState(signature, result)

	if err := amqpBackend.updateState(taskState); err != nil {
		return nil, err
	}

	if signature.GroupUUID == "" {
		return nil, nil
	}

	return amqpBackend.updateStateGroup(
		signature.GroupUUID,
		signature.GroupTaskCount,
		taskState,
	)
}

// SetStateFailure - sets task state to FAILURE
func (amqpBackend *AMQPBackend) SetStateFailure(signature *signatures.TaskSignature, err string) error {
	taskState := NewFailureTaskState(signature, err)
	return amqpBackend.updateState(taskState)
}

// GetState returns the latest task state. It will only return the status once
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
func (amqpBackend *AMQPBackend) PurgeState(taskState *TaskState) error {
	return amqpBackend.deleteQueue(taskState.TaskUUID)
}

// PurgeStateGroup - deletes stored task state
func (amqpBackend *AMQPBackend) PurgeStateGroup(taskStateGroup *TaskStateGroup) error {
	return amqpBackend.deleteQueue(taskStateGroup.GroupUUID)
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

// NOTE: Using AMQP as a result backend is quite tricky since every time we
// read a message from the queue keeping task states, the message is removed
// from the queue. This leads to problems with keeping a reliable state of a
// group of tasks since concurrent processes updating the group state cause
// race conditions and inconsistent state.
//
// This is avoided by a "clever" hack. We only call updateStateGroup when a
// task state is set to SUCCESS and instead of TaskStateGroup object we only
// store a serialised TaskState in the group queue. After publishing the
// SUCCESS state of a task to the queue we use channel.QueueInspect to get
// the number of unacknowledged messages in the queue, if it is equal to the
// total number of tasks in the group we can safely assume all tasks succeeded
// and return a TaskStateGroup object with all successful states.
func (amqpBackend *AMQPBackend) updateStateGroup(groupUUID string, groupTaskCount int, taskState *TaskState) (*TaskStateGroup, error) {
	if groupUUID == "" || groupTaskCount == 0 {
		return nil, nil
	}

	conn, channel, queue, confirmsChan, err := amqpBackend.open(groupUUID)
	if err != nil {
		return nil, err
	}

	defer amqpBackend.close(channel, conn)

	message, err := json.Marshal(taskState)
	if err != nil {
		return nil, fmt.Errorf("JSON Encode Message: %v", err)
	}

	if err := channel.Publish(
		amqpBackend.config.Exchange, // exchange
		groupUUID,                   // routing key
		false,                       // mandatory
		false,                       // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         message,
			DeliveryMode: amqp.Persistent, // Persistent // Transient
		},
	); err != nil {
		return nil, err
	}

	confirmed := <-confirmsChan

	if !confirmed.Ack {
		return nil, fmt.Errorf("Failed delivery of delivery tag: %v", confirmed.DeliveryTag)
	}

	queueState, err := channel.QueueInspect(groupUUID)
	if err != nil {
		return nil, fmt.Errorf("Queue Inspect: %v", err)
	}

	taskStateGroup := &TaskStateGroup{
		GroupUUID:      groupUUID,
		GroupTaskCount: groupTaskCount,
		States:         make(map[string]*TaskState),
	}

	if queueState.Messages != groupTaskCount {
		return taskStateGroup, nil
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
		return taskStateGroup, fmt.Errorf("Queue Consume: %s", err)
	}

	for i := 0; i < groupTaskCount; i++ {
		d := <-deliveries

		taskState = &TaskState{}
		if err := json.Unmarshal([]byte(d.Body), &taskState); err != nil {
			return taskStateGroup, err
		}

		taskStateGroup.States[taskState.TaskUUID] = taskState
	}

	return taskStateGroup, nil
}

// Connects to the message queue, opens a channel, declares a queue
func (amqpBackend *AMQPBackend) open(taskUUID string) (*amqp.Connection, *amqp.Channel, amqp.Queue, <-chan amqp.Confirmation, error) {
	var conn *amqp.Connection
	var channel *amqp.Channel
	var queue amqp.Queue
	var err error

	conn, err = amqp.Dial(amqpBackend.config.ResultBackend)
	if err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Dial: %s", err)
	}

	channel, err = conn.Channel()
	if err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Channel: %s", err)
	}

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
		return conn, channel, queue, nil, fmt.Errorf("Exchange: %s", err)
	}

	resultsExpireIn := amqpBackend.config.ResultsExpireIn * 1000
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
		return conn, channel, queue, nil, fmt.Errorf("Queue Declare: %s", err)
	}

	if err := channel.QueueBind(
		queue.Name,                  // name of the queue
		taskUUID,                    // binding key
		amqpBackend.config.Exchange, // source exchange
		false, // noWait
		nil,   // arguments
	); err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Queue Bind: %s", err)
	}

	confirmsChan := make(chan amqp.Confirmation, 1)

	// Enable publish confirmations
	if err := channel.Confirm(false); err != nil {
		close(confirmsChan)
		return conn, channel, queue, nil, fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}

	return conn, channel, queue, channel.NotifyPublish(confirmsChan), nil
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
