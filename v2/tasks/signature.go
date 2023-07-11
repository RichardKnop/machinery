package tasks

import (
	"fmt"
	"time"

	"github.com/RichardKnop/machinery/v2/utils"

	"github.com/google/uuid"
)

// Arg represents a single argument passed to invocation fo a task
type Arg struct {
	Name  string      `json:"name,omitempty" bson:"name"`
	Type  string      `json:"type,omitempty" bson:"type"`
	Value interface{} `json:"value,omitempty" bson:"value"`
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

// Signature represents a single task invocation
type Signature struct {
	UUID           string       `json:"UUID,omitempty"`
	Name           string       `json:"name,omitempty"`
	RoutingKey     string       `json:"routingKey,omitempty"`
	ETA            *time.Time   `json:"ETA,omitempty"`
	GroupUUID      string       `json:"groupUUID,omitempty"`
	GroupTaskCount int          `json:"groupTaskCount,omitempty"`
	Args           []Arg        `json:"args,omitempty"`
	Headers        Headers      `json:"headers,omitempty"`
	Priority       uint8        `json:"priority,omitempty"`
	Immutable      bool         `json:"immutable,omitempty"`
	RetryCount     int          `json:"retryCount,omitempty"`
	RetryTimeout   int          `json:"retryTimeout,omitempty"`
	OnSuccess      []*Signature `json:"onSuccess,omitempty"`
	OnError        []*Signature `json:"onError,omitempty"`
	ChordCallback  *Signature   `json:"chordCallback,omitempty"`
	//MessageGroupId for Broker, e.g. SQS
	BrokerMessageGroupId string `json:"brokerMessageGroupId,omitempty"`
	//ReceiptHandle of SQS Message
	SQSReceiptHandle string `json:"SQSReceiptHandle,omitempty"`
	// StopTaskDeletionOnError used with sqs when we want to send failed messages to dlq,
	// and don't want machinery to delete from source queue
	StopTaskDeletionOnError bool `json:"stopTaskDeletionOnError,omitempty"`
	// IgnoreWhenTaskNotRegistered auto removes the request when there is no handeler available
	// When this is true a task with no handler will be ignored and not placed back in the queue
	IgnoreWhenTaskNotRegistered bool `json:"ignoreWhenTaskNotRegistered,omitempty"`
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
