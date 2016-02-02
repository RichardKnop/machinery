package brokers

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/RichardKnop/machinery/v1/signatures"
)

type EagerBroker struct {
	worker TaskProcessor
}

func NewEagerBroker() Broker {
	return &EagerBroker{}
}

type EagerMode interface {
	AssignWorker(p TaskProcessor)
}

//
// Broker interface
//
func (e *EagerBroker) SetRegisteredTaskNames(names []string) {
	// do nothing
}

func (e *EagerBroker) IsTaskRegistered(name string) bool {
	return true
}

func (e *EagerBroker) StartConsuming(consumerTag string, p TaskProcessor) (bool, error) {
	return true, nil
}

func (e *EagerBroker) StopConsuming() {
	// do nothing
}

func (e *EagerBroker) Publish(task *signatures.TaskSignature) error {
	if e.worker == nil {
		return errors.New("worker is not assigned in eager-mode")
	}

	// faking the behavior to marshal input into json
	// and unmarshal it back
	message, err := json.Marshal(task)
	if err != nil {
		return errors.New(fmt.Sprintf("json marshaling failed: %v", err))
	}

	sig_ := signatures.TaskSignature{}
	err = json.Unmarshal(message, &sig_)
	if err != nil {
		return errors.New(fmt.Sprintf("json unmarshaling failed: %v", err))
	}

	// blocking call to the task directly
	return e.worker.Process(&sig_)
}

//
// Eager interface
//
func (e *EagerBroker) AssignWorker(p TaskProcessor) {
	e.worker = p
}
