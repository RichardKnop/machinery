package tasks

import (
	"time"
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
	OnSuccess      []*Signature
	OnError        []*Signature
	ChordCallback  *Signature
}

// AdjustRoutingKey makes sure the routing key is correct.
// If the routing key is an empty string:
// a) set it to binding key for direct exchange type
// b) set it to default queue name
func (s *Signature) AdjustRoutingKey(exchangeType, bindingKey, queueName string) {
	if s.RoutingKey != "" {
		return
	}

	if exchangeType == "direct" {
		s.RoutingKey = bindingKey
		return
	}

	s.RoutingKey = queueName
}
