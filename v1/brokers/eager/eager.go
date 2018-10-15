package eager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// Broker represents an "eager" in-memory broker
type Broker struct {
	worker iface.TaskProcessor
	common.Broker
}

// New creates new Broker instance
func New() iface.Broker {
	return new(Broker)
}

// Mode interface with methods specific for this broker
type Mode interface {
	AssignWorker(p iface.TaskProcessor)
}

// StartConsuming enters a loop and waits for incoming messages
func (eagerBroker *Broker) StartConsuming(consumerTag string, concurrency int, p iface.TaskProcessor) (bool, error) {
	return true, nil
}

// StopConsuming quits the loop
func (eagerBroker *Broker) StopConsuming() {
	// do nothing
}

// Publish places a new message on the default queue
func (eagerBroker *Broker) Publish(task *tasks.Signature) error {
	if eagerBroker.worker == nil {
		return errors.New("worker is not assigned in eager-mode")
	}

	// faking the behavior to marshal input into json
	// and unmarshal it back
	message, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(message))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return fmt.Errorf("JSON unmarshal error: %s", err)
	}

	// blocking call to the task directly
	return eagerBroker.worker.Process(signature)
}

// GetPendingTasks returns a slice of task.Signatures waiting in the queue
func (eagerBroker *Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	return []*tasks.Signature{}, errors.New("Not implemented")
}

// AssignWorker assigns a worker to the eager broker
func (eagerBroker *Broker) AssignWorker(w iface.TaskProcessor) {
	eagerBroker.worker = w
}
