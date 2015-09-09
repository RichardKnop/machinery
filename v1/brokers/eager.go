package brokers

import (
	"errors"

	"github.com/RichardKnop/machinery/v1/backends"
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
	// blocking call to the task directly
	return e.worker.Process(task)
}

//
// Eager interface
//
func (e *EagerBroker) AssignWorker(p TaskProcessor) {
	e.worker = p
}
