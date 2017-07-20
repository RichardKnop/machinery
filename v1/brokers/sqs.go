package brokers

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// SQSBroker represents a SQS broker
type SQSBroker struct {
	Broker
	sess *session.Session
}

// SQSNew creates new Broker instance
func NewSQSBroker(cnf *config.Config) Interface {

	b := &SQSBroker{Broker: New(cnf)}
	b.sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	return b
}

// SetRegisteredTaskNames sets registered task names
func (b *SQSBroker) SetRegisteredTaskNames(names []string) {
	b.registeredTaskNames = names
}

// IsTaskRegistered returns true if the task is registered with this broker
func (b *SQSBroker) IsTaskRegistered(name string) bool {
	for _, registeredTaskName := range b.registeredTaskNames {
		if registeredTaskName == name {
			return true
		}
	}
	return false
}

// GetPendingTasks returns a slice of task.Signatures waiting in the queue
func (b *SQSBroker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	return nil, errors.New("Not implemented")
}

// StartConsuming enters a loop and waits for incoming messages
func (b *SQSBroker) StartConsuming(consumerTag string, p TaskProcessor) (bool, error) {
	// b.startConsuming(consumerTag, taskProcessor)
	return true, nil
}

// StopConsuming quits the loop
func (b *SQSBroker) StopConsuming() {
	// do nothing
}

// Publish places a new message on the default queue
func (b *SQSBroker) Publish(signature *tasks.Signature) error {

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// Check that signature.RoutingKey is set, if not switch to DefaultQueue
	b.AdjustRoutingKey(signature)

	// Create a SQS service client.
	svc := sqs.New(b.sess)

	// Use Machinery's signature Group UUID as SQS Message Group ID.
	MsgGroupID := signature.GroupUUID

	// Use Machinery's signature Task UUID as SQS Message Group ID.
	MsgDedupID := signature.UUID

	MsgInput := &sqs.SendMessageInput{
		MessageGroupId:         aws.String(MsgGroupID),
		MessageDeduplicationId: aws.String(MsgDedupID),
		MessageBody:            aws.String(string(msg)),
		QueueUrl:               aws.String(b.cnf.Broker + "/" + signature.RoutingKey),
	}

	// Check the ETA signature field, if it is set and it is in the future,
	// and is not a fifo queue, set a delay in seconds for the task.
	if signature.ETA != nil && !strings.HasSuffix(signature.RoutingKey, ".fifo") {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			MsgInput.DelaySeconds = aws.Int64(signature.ETA.Unix() - now.Unix())
		}
	}

	result, err := svc.SendMessage(MsgInput)

	if err == nil {
		fmt.Println("Success", *result.MessageId)
	}

	fmt.Println("Error", err)
	return err
}

// TODO: Add GetPendingTasks() & add AssignWorker(), refer to
// RichardKnop's broker.
