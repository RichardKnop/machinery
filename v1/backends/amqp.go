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

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/streadway/amqp"
)

// AMQPBackend represents an AMQP result backend
type AMQPBackend struct {
	cnf *config.Config
}

// NewAMQPBackend creates AMQPBackend instance
func NewAMQPBackend(cnf *config.Config) Interface {
	return &AMQPBackend{cnf: cnf}
}

// InitGroup creates and saves a group meta data object
func (b *AMQPBackend) InitGroup(groupUUID string, taskUUIDs []string) error {
	return nil
}

// GroupCompleted returns true if all tasks in a group finished
// NOTE: Given AMQP limitation this will only return true if all finished
// tasks were successful as we do not keep track of completed failed tasks
func (b *AMQPBackend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	conn, channel, err := b.open()
	if err != nil {
		return false, err
	}
	defer b.close(channel, conn)

	queueState, err := b.inspectQueue(channel, groupUUID)
	if err != nil {
		return false, nil
	}

	return queueState.Messages == groupTaskCount, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *AMQPBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	conn, channel, queue, _, err := b.connect(groupUUID)
	if err != nil {
		return nil, err
	}
	defer b.close(channel, conn)

	queueState, err := channel.QueueInspect(groupUUID)
	if err != nil {
		return nil, fmt.Errorf("Queue Inspect: %v", err)
	}

	if queueState.Messages != groupTaskCount {
		return nil, fmt.Errorf("Already consumed: %v", err)
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
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}

	states := make([]*tasks.TaskState, groupTaskCount)
	for i := 0; i < groupTaskCount; i++ {
		d := <-deliveries

		state := new(tasks.TaskState)

		if err := json.Unmarshal([]byte(d.Body), state); err != nil {
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
func (b *AMQPBackend) TriggerChord(groupUUID string) (bool, error) {
	conn, channel, err := b.open()
	if err != nil {
		return false, err
	}
	defer b.close(channel, conn)

	_, err = b.inspectQueue(channel, fmt.Sprintf("%s_chord_triggered", groupUUID))
	if err != nil {
		return true, nil
	}

	return false, nil
}

// SetStatePending updates task state to PENDING
func (b *AMQPBackend) SetStatePending(signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	return b.updateState(taskState)
}

// SetStateReceived updates task state to RECEIVED
func (b *AMQPBackend) SetStateReceived(signature *tasks.Signature) error {
	taskState := tasks.NewReceivedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateStarted updates task state to STARTED
func (b *AMQPBackend) SetStateStarted(signature *tasks.Signature) error {
	taskState := tasks.NewStartedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateSuccess updates task state to SUCCESS
func (b *AMQPBackend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
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
func (b *AMQPBackend) SetStateFailure(signature *tasks.Signature, err string) error {
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
func (b *AMQPBackend) GetState(taskUUID string) (*tasks.TaskState, error) {
	conn, channel, queue, _, err := b.connect(taskUUID)
	if err != nil {
		return nil, err
	}
	defer b.close(channel, conn)

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

	state := new(tasks.TaskState)
	if err := json.Unmarshal([]byte(d.Body), state); err != nil {
		log.ERROR.Printf("Failed to unmarshal task state: %v", string(d.Body))
		log.ERROR.Print(err)
		return nil, err
	}

	return state, nil
}

// PurgeState deletes stored task state
func (b *AMQPBackend) PurgeState(taskUUID string) error {
	return b.deleteQueue(taskUUID)
}

// PurgeGroupMeta deletes stored group meta data
func (b *AMQPBackend) PurgeGroupMeta(groupUUID string) error {
	b.deleteQueue(fmt.Sprintf("%s_chord_triggered", groupUUID))
	return b.deleteQueue(groupUUID)
}

// updateState saves current task state
func (b *AMQPBackend) updateState(taskState *tasks.TaskState) error {
	message, err := json.Marshal(taskState)
	if err != nil {
		return fmt.Errorf("JSON Encode Message: %v", err)
	}

	conn, channel, _, confirmsChan, err := b.connect(taskState.TaskUUID)
	if err != nil {
		return err
	}
	defer b.close(channel, conn)

	if err := channel.Publish(
		b.cnf.AMQP.Exchange, // exchange
		taskState.TaskUUID,  // routing key
		false,               // mandatory
		false,               // immediate
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

// getExpiresIn returns expiration time
func (b *AMQPBackend) getExpiresIn() int {
	resultsExpireIn := b.cnf.ResultsExpireIn * 1000
	if resultsExpireIn == 0 {
		// // expire results after 1 hour by default
		resultsExpireIn = 3600 * 1000
	}
	return resultsExpireIn
}

// deleteQueue removes a queue with a name
func (b *AMQPBackend) deleteQueue(queueName string) error {
	conn, channel, queue, _, err := b.connect(queueName)
	if err != nil {
		return err
	}
	defer b.close(channel, conn)

	// First return value is number of messages removed
	_, err = channel.QueueDelete(
		queue.Name, // name
		false,      // ifUnused
		false,      // ifEmpty
		false,      // noWait
	)

	return err
}

// markTaskCompleted marks task as completed in either groupdUUID_success
// or groupUUID_failure queue. This is important for GroupCompleted and
// GroupSuccessful methods
func (b *AMQPBackend) markTaskCompleted(signature *tasks.Signature, taskState *tasks.TaskState) error {
	if signature.GroupUUID == "" || signature.GroupTaskCount == 0 {
		return nil
	}

	message, err := json.Marshal(taskState)
	if err != nil {
		return fmt.Errorf("JSON Encode Message: %v", err)
	}

	conn, channel, _, confirmsChan, err := b.connect(signature.GroupUUID)
	if err != nil {
		return err
	}
	defer b.close(channel, conn)

	if err := channel.Publish(
		b.cnf.AMQP.Exchange, // exchange
		signature.GroupUUID, // routing key
		false,               // mandatory
		false,               // immediate
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

// connect opens a connection to RabbitMQ, declares an exchange, opens a channel,
// declares and binds the queue and enables publish notifications
func (b *AMQPBackend) connect(queueName string) (*amqp.Connection, *amqp.Channel, amqp.Queue, <-chan amqp.Confirmation, error) {
	var (
		conn    *amqp.Connection
		channel *amqp.Channel
		queue   amqp.Queue
		err     error
	)

	// Connect to server
	conn, channel, err = b.open()
	if err != nil {
		return conn, channel, queue, nil, err
	}

	// Declare an exchange
	err = channel.ExchangeDeclare(
		b.cnf.AMQP.Exchange,     // name of the exchange
		b.cnf.AMQP.ExchangeType, // type
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
		"x-message-ttl": int32(b.getExpiresIn()),
	}
	queue, err = channel.QueueDeclare(
		queueName, // name
		false,     // durable
		true,      // delete when unused
		false,     // exclusive
		false,     // no-wait
		arguments,
	)
	if err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Queue Declare: %s", err)
	}

	// Bind the queue
	if err := channel.QueueBind(
		queue.Name,          // name of the queue
		queueName,           // binding key
		b.cnf.AMQP.Exchange, // source exchange
		false,               // noWait
		nil,                 // arguments
	); err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Queue Bind: %s", err)
	}

	// Enable publish confirmations
	if err := channel.Confirm(false); err != nil {
		return conn, channel, queue, nil, fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}

	return conn, channel, queue, channel.NotifyPublish(make(chan amqp.Confirmation, 1)), nil
}

// open new RabbitMQ connection
func (b *AMQPBackend) open() (*amqp.Connection, *amqp.Channel, error) {
	var (
		conn    *amqp.Connection
		channel *amqp.Channel
		err     error
	)

	// Connect
	// From amqp docs: DialTLS will use the provided tls.Config when it encounters an amqps:// scheme
	// and will dial a plain connection when it encounters an amqp:// scheme.
	conn, err = amqp.DialTLS(b.cnf.Broker, b.cnf.TLSConfig)
	if err != nil {
		return conn, channel, fmt.Errorf("Dial: %s", err)
	}

	// Open a channel
	channel, err = conn.Channel()
	if err != nil {
		return conn, channel, fmt.Errorf("Channel: %s", err)
	}

	return conn, channel, nil
}

// inspect a specific queue
func (b *AMQPBackend) inspectQueue(channel *amqp.Channel, queueName string) (*amqp.Queue, error) {
	var (
		queueState amqp.Queue
		err        error
	)

	queueState, err = channel.QueueInspect(queueName)
	if err != nil {
		return nil, fmt.Errorf("Queue Inspect: %v", err)
	}

	return &queueState, nil
}

// close connection
func (b *AMQPBackend) close(channel *amqp.Channel, conn *amqp.Connection) error {
	if channel != nil {
		if err := channel.Close(); err != nil {
			return fmt.Errorf("Channel Close: %s", err)
		}
	}

	if conn != nil {
		if err := conn.Close(); err != nil {
			return fmt.Errorf("Connection Close: %s", err)
		}
	}

	return nil
}
