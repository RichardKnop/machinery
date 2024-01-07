package amqp

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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Backend represents an AMQP result backend
type Backend struct {
	common.Backend
	common.AMQPConnector
}

// New creates Backend instance
func New(cnf *config.Config) iface.Backend {
	return &Backend{Backend: common.NewBackend(cnf), AMQPConnector: common.AMQPConnector{}}
}

// InitGroup creates and saves a group meta data object
func (b *Backend) InitGroup(groupUUID string, taskUUIDs []string) error {
	return nil
}

// GroupCompleted returns true if all tasks in a group finished
// NOTE: Given AMQP limitation this will only return true if all finished
// tasks were successful as we do not keep track of completed failed tasks
func (b *Backend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	conn, channel, err := b.Open(b.GetConfig().ResultBackend, b.GetConfig().TLSConfig)
	if err != nil {
		return false, err
	}
	defer b.Close(channel, conn)

	queueState, err := b.InspectQueue(channel, groupUUID)
	if err != nil {
		return false, nil
	}

	return queueState.Messages == groupTaskCount, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *Backend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	conn, channel, err := b.Open(b.GetConfig().ResultBackend, b.GetConfig().TLSConfig)
	if err != nil {
		return nil, err
	}
	defer b.Close(channel, conn)

	queueState, err := b.InspectQueue(channel, groupUUID)
	if err != nil {
		return nil, err
	}

	if queueState.Messages != groupTaskCount {
		return nil, fmt.Errorf("Already consumed: %v", err)
	}

	deliveries, err := channel.Consume(
		groupUUID, // queue name
		"",        // consumer tag
		false,     // auto-ack
		true,      // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue consume error: %s", err)
	}

	states := make([]*tasks.TaskState, groupTaskCount)
	for i := 0; i < groupTaskCount; i++ {
		d := <-deliveries

		state := new(tasks.TaskState)
		decoder := json.NewDecoder(bytes.NewReader([]byte(d.Body)))
		decoder.UseNumber()
		if err := decoder.Decode(state); err != nil {
			d.Nack(false, false) // multiple, requeue
			return nil, err
		}

		d.Ack(false) // multiple

		states[i] = state
	}

	return states, nil
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *Backend) TriggerChord(groupUUID string) (bool, error) {
	conn, channel, err := b.Open(b.GetConfig().ResultBackend, b.GetConfig().TLSConfig)
	if err != nil {
		return false, err
	}
	defer b.Close(channel, conn)

	_, err = b.InspectQueue(channel, amqmChordTriggeredQueue(groupUUID))
	if err != nil {
		return true, nil
	}

	return false, nil
}

// SetStatePending updates task state to PENDING
func (b *Backend) SetStatePending(signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	return b.updateState(taskState)
}

// SetStateReceived updates task state to RECEIVED
func (b *Backend) SetStateReceived(signature *tasks.Signature) error {
	taskState := tasks.NewReceivedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateStarted updates task state to STARTED
func (b *Backend) SetStateStarted(signature *tasks.Signature) error {
	taskState := tasks.NewStartedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateRetry updates task state to RETRY
func (b *Backend) SetStateRetry(signature *tasks.Signature) error {
	state := tasks.NewRetryTaskState(signature)
	return b.updateState(state)
}

// SetStateSuccess updates task state to SUCCESS
func (b *Backend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	taskState := tasks.NewSuccessTaskState(signature, results)

	if err := b.updateState(taskState); err != nil {
		return err
	}

	if signature.GroupUUID == "" {
		return nil
	}

	return b.markTaskCompleted(signature, taskState)
}

// SetStateFailure updates task state to FAILURE
func (b *Backend) SetStateFailure(signature *tasks.Signature, err string) error {
	taskState := tasks.NewFailureTaskState(signature, err)

	if err := b.updateState(taskState); err != nil {
		return err
	}

	if signature.GroupUUID == "" {
		return nil
	}

	return b.markTaskCompleted(signature, taskState)
}

// GetState returns the latest task state. It will only return the status once
// as the message will get consumed and removed from the queue.
func (b *Backend) GetState(taskUUID string) (*tasks.TaskState, error) {
	declareQueueArgs := amqp.Table{
		// Time in milliseconds
		// after that message will expire
		"x-message-ttl": int32(b.getExpiresIn()),
		// Time after that the queue will be deleted.
		"x-expires": int32(b.getExpiresIn()),
	}
	conn, channel, _, _, _, err := b.Connect(
		b.GetConfig().ResultBackend,
		"",
		b.GetConfig().TLSConfig,
		b.GetConfig().AMQP.Exchange,     // exchange name
		b.GetConfig().AMQP.ExchangeType, // exchange type
		taskUUID,                        // queue name
		false,                           // queue durable
		true,                            // queue delete when unused
		taskUUID,                        // queue binding key
		nil,                             // exchange declare args
		declareQueueArgs,                // queue declare args
		nil,                             // queue binding args
	)
	if err != nil {
		return nil, err
	}
	defer b.Close(channel, conn)

	d, ok, err := channel.Get(
		taskUUID, // queue name
		false,    // multiple
	)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("No state ready")
	}

	d.Ack(false)

	state := new(tasks.TaskState)
	decoder := json.NewDecoder(bytes.NewReader([]byte(d.Body)))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		log.ERROR.Printf("Failed to unmarshal task state: %s", string(d.Body))
		log.ERROR.Print(err)
		return nil, err
	}

	return state, nil
}

// PurgeState deletes stored task state
func (b *Backend) PurgeState(taskUUID string) error {
	conn, channel, err := b.Open(b.GetConfig().ResultBackend, b.GetConfig().TLSConfig)
	if err != nil {
		return err
	}
	defer b.Close(channel, conn)

	return b.DeleteQueue(channel, taskUUID)
}

// PurgeGroupMeta deletes stored group meta data
func (b *Backend) PurgeGroupMeta(groupUUID string) error {
	conn, channel, err := b.Open(b.GetConfig().ResultBackend, b.GetConfig().TLSConfig)
	if err != nil {
		return err
	}
	defer b.Close(channel, conn)

	b.DeleteQueue(channel, amqmChordTriggeredQueue(groupUUID))

	return b.DeleteQueue(channel, groupUUID)
}

// updateState saves current task state
func (b *Backend) updateState(taskState *tasks.TaskState) error {
	message, err := json.Marshal(taskState)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	declareQueueArgs := amqp.Table{
		// Time in milliseconds
		// after that message will expire
		"x-message-ttl": int32(b.getExpiresIn()),
		// Time after that the queue will be deleted.
		"x-expires": int32(b.getExpiresIn()),
	}
	conn, channel, queue, confirmsChan, _, err := b.Connect(
		b.GetConfig().ResultBackend,
		"",
		b.GetConfig().TLSConfig,
		b.GetConfig().AMQP.Exchange,     // exchange name
		b.GetConfig().AMQP.ExchangeType, // exchange type
		taskState.TaskUUID,              // queue name
		false,                           // queue durable
		true,                            // queue delete when unused
		taskState.TaskUUID,              // queue binding key
		nil,                             // exchange declare args
		declareQueueArgs,                // queue declare args
		nil,                             // queue binding args
	)
	if err != nil {
		return err
	}
	defer b.Close(channel, conn)

	if err := channel.Publish(
		b.GetConfig().AMQP.Exchange, // exchange
		queue.Name,                  // routing key
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

	return fmt.Errorf("Failed delivery of delivery tag: %d", confirmed.DeliveryTag)
}

// getExpiresIn returns expiration time
func (b *Backend) getExpiresIn() int {
	resultsExpireIn := b.GetConfig().ResultsExpireIn * 1000
	if resultsExpireIn == 0 {
		// // expire results after 1 hour by default
		resultsExpireIn = config.DefaultResultsExpireIn * 1000
	}
	return resultsExpireIn
}

// markTaskCompleted marks task as completed in either groupdUUID_success
// or groupUUID_failure queue. This is important for GroupCompleted and
// GroupSuccessful methods
func (b *Backend) markTaskCompleted(signature *tasks.Signature, taskState *tasks.TaskState) error {
	if signature.GroupUUID == "" || signature.GroupTaskCount == 0 {
		return nil
	}

	message, err := json.Marshal(taskState)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	declareQueueArgs := amqp.Table{
		// Time in milliseconds
		// after that message will expire
		"x-message-ttl": int32(b.getExpiresIn()),
		// Time after that the queue will be deleted.
		"x-expires": int32(b.getExpiresIn()),
	}
	conn, channel, queue, confirmsChan, _, err := b.Connect(
		b.GetConfig().ResultBackend,
		"",
		b.GetConfig().TLSConfig,
		b.GetConfig().AMQP.Exchange,     // exchange name
		b.GetConfig().AMQP.ExchangeType, // exchange type
		signature.GroupUUID,             // queue name
		false,                           // queue durable
		true,                            // queue delete when unused
		signature.GroupUUID,             // queue binding key
		nil,                             // exchange declare args
		declareQueueArgs,                // queue declare args
		nil,                             // queue binding args
	)
	if err != nil {
		return err
	}
	defer b.Close(channel, conn)

	if err := channel.Publish(
		b.GetConfig().AMQP.Exchange, // exchange
		queue.Name,                  // routing key
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

func amqmChordTriggeredQueue(groupUUID string) string {
	return fmt.Sprintf("%s_chord_triggered", groupUUID)
}
