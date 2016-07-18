package brokers

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/RichardKnop/machinery/v1/signatures"
)

// EagerBroker represents an "eager" in-memory broker
type EagerBroker struct {
	worker TaskProcessor
}

// NewEagerBroker creates new EagerBroker instance
func NewEagerBroker() Broker {
	return new(EagerBroker)
}

// EagerMode interface with methods specific for this broker
type EagerMode interface {
	AssignWorker(p TaskProcessor)
}

// SetRegisteredTaskNames sets registered task names
func (eagerBroker *EagerBroker) SetRegisteredTaskNames(names []string) {
	// do nothing
}

// IsTaskRegistered returns true if the task is registered with this broker
func (eagerBroker *EagerBroker) IsTaskRegistered(name string) bool {
	return true
}

// StartConsuming enters a loop and waits for incoming messages
func (eagerBroker *EagerBroker) StartConsuming(consumerTag string, p TaskProcessor) (bool, error) {
	return true, nil
}

// StopConsuming quits the loop
func (eagerBroker *EagerBroker) StopConsuming() {
	// do nothing
}

// Publish places a new message on the default queue
func (eagerBroker *EagerBroker) Publish(task *signatures.TaskSignature) error {
	if eagerBroker.worker == nil {
		return errors.New("worker is not assigned in eager-mode")
	}

	// faking the behavior to marshal input into json
	// and unmarshal it back
	message, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("json marshaling failed: %v", err)
	}

	signature := new(signatures.TaskSignature)
	err = json.Unmarshal(message, &signature)
	if err != nil {
		return fmt.Errorf("json unmarshaling failed: %v", err)
	}

	// blocking call to the task directly
	return eagerBroker.worker.Process(signature)
}

// GetPendingTasks returns a slice of task.Signatures waiting in the queue
func (eagerBroker *EagerBroker) GetPendingTasks(queue string) ([]*signatures.TaskSignature, error) {
	return []*signatures.TaskSignature{}, errors.New("Not implemented")
}

// AssignWorker assigns a worker to the eager broker
func (eagerBroker *EagerBroker) AssignWorker(w TaskProcessor) {
	eagerBroker.worker = w
}
