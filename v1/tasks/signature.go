package tasks

import (
	"fmt"
	"time"

	"github.com/satori/go.uuid"
)

// Arg represents a single argument passed to invocation fo a task
type Arg struct {
	Type  string
	Value interface{}
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
}

// NewSignature creates a new task signature
func NewSignature(name string, args []Arg) *Signature {
	return &Signature{
		UUID: fmt.Sprintf("task_%v", uuid.NewV4()),
		Name: name,
		Args: args,
	}
}
