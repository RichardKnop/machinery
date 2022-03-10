package tasks

import (
	"fmt"
	"time"

	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/retry"
	"github.com/RichardKnop/machinery/v1/utils"

	"github.com/google/uuid"
)

// Arg represents a single argument passed to invocation fo a task
type Arg struct {
	Name  string      `bson:"name"`
	Type  string      `bson:"type"`
	Value interface{} `bson:"value"`
}

// Headers represents the headers which should be used to direct the task
type Headers map[string]interface{}

// Set on Headers implements opentracing.TextMapWriter for trace propagation
func (h Headers) Set(key, val string) {
	h[key] = val
}

// ForeachKey on Headers implements opentracing.TextMapReader for trace propagation.
// It is essentially the same as the opentracing.TextMapReader implementation except
// for the added casting from interface{} to string.
func (h Headers) ForeachKey(handler func(key, val string) error) error {
	for k, v := range h {
		// Skip any non string values
		stringValue, ok := v.(string)
		if !ok {
			continue
		}

		if err := handler(k, stringValue); err != nil {
			return err
		}
	}

	return nil
}

type MsgType int

const (
	NOTIFICATION = MsgType(0)
	PUSH         = MsgType(1)
	EMAIL        = MsgType(2)
	SMS          = MsgType(3)
)

// Signature represents a single task invocation
type Signature struct {
	UUID           string       `bson:"uuid,omitempty"`
	Name           string       `bson:"name,omitempty"`
	RoutingKey     string       `bson:"routingKey,omitempty"`
	ETA            *time.Time   `bson:"eta,omitempty"`
	GroupUUID      string       `bson:"groupuuid,omitempty"`
	GroupTaskCount int          `bson:"groupTaskCount,omitempty"`
	Args           []Arg        `bson:"args,omitempty"`
	Headers        Headers      `bson:"headers,omitempty"`
	Priority       uint8        `bson:"priority,omitempty"`
	Immutable      bool         `bson:"immutable,omitempty"`
	RetryCount     int          `bson:"retryCount,omitempty"`
	RetryTimeout   int          `bson:"retryTimeout,omitempty"`
	OnSuccess      []*Signature `bson:"onSuccess,omitempty"`
	OnError        []*Signature `bson:"onError,omitempty"`
	ChordCallback  *Signature   `bson:"chordCallback,omitempty"`
	//MessageGroupId for Broker, e.g. SQS
	BrokerMessageGroupId string `bson:"brokerMessageGroupId,omitempty"`
	//ReceiptHandle of SQS Message
	SQSReceiptHandle string `bson:"sqsReceiptHandle,omitempty"`
	// StopTaskDeletionOnError used with sqs when we want to send failed messages to dlq,
	// and don't want machinery to delete from source queue
	StopTaskDeletionOnError bool `bson:"stopTaskDeletionOnError,omitempty"`
	// IgnoreWhenTaskNotRegistered auto removes the request when there is no handeler available
	// When this is true a task with no handler will be ignored and not placed back in the queue
	IgnoreWhenTaskNotRegistered bool    `bson:"ignoreWhenTaskNotRegistered,omitempty"`
	MsgType                     MsgType `bson:"msgType,omitempty"`
}

func (s *Signature) NextRetryTimeout() (retryTimeout int, err error) {
	switch s.MsgType {
	case NOTIFICATION:
		retryTimeout, err = retry.TransNotificationBackoff(s.RetryCount)
		s.RetryTimeout = retryTimeout
	case PUSH:
		log.INFO.Print("i am push")
	case EMAIL:
		log.INFO.Print("i am email")
	case SMS:
		log.INFO.Print("i am sms")
	default:
		log.INFO.Print("i am neither")
		retryTimeout = retry.FibonacciNext(s.RetryTimeout)
		s.RetryTimeout = retryTimeout
	}
	return
}

// NewSignature creates a new task signature
func NewSignature(name string, args []Arg) (*Signature, error) {
	signatureID := uuid.New().String()
	return &Signature{
		UUID: fmt.Sprintf("task_%v", signatureID),
		Name: name,
		Args: args,
	}, nil
}

func CopySignatures(signatures ...*Signature) []*Signature {
	var sigs = make([]*Signature, len(signatures))
	for index, signature := range signatures {
		sigs[index] = CopySignature(signature)
	}
	return sigs
}

func CopySignature(signature *Signature) *Signature {
	var sig = new(Signature)
	_ = utils.DeepCopy(sig, signature)
	return sig
}
