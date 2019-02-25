package tasks

import (
	"fmt"
	"time"

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

// Signature represents a single task invocation
type Signature struct {
	UUID           string
	Name           string
	RoutingKey     string
	ETA            *time.Time
	GroupUUID      string
	GroupTaskCount int
	Args           []Arg
	Headers        Headers
	Immutable      bool
	RetryCount     int
	RetryTimeout   int
	OnSuccess      []*Signature
	OnError        []*Signature
	ChordCallback  *Signature
	//MessageGroupId for Broker, e.g. SQS
	BrokerMessageGroupId string
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
