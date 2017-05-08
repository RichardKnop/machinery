package brokers

import (
	"errors"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/retry"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// Broker represents a base broker structure
type Broker struct {
	cnf                 *config.Config
	registeredTaskNames []string
	retry               bool
	retryFunc           func()
	stopChan            chan int
}

// SetRegisteredTaskNames sets registered task names
func (b *Broker) SetRegisteredTaskNames(names []string) {
	b.registeredTaskNames = names
}

// IsTaskRegistered returns true if the task is registered with this broker
func (b *Broker) IsTaskRegistered(name string) bool {
	for _, registeredTaskName := range b.registeredTaskNames {
		if registeredTaskName == name {
			return true
		}
	}
	return false
}

// GetPendingTasks returns a slice of task.Signatures waiting in the queue
func (b *Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	return nil, errors.New("Not implemented")
}

// startConsuming is a common part of StartConsuming method
func (b *Broker) startConsuming(consumerTag string, taskProcessor TaskProcessor) {
	if b.retryFunc == nil {
		b.retryFunc = retry.Closure()
	}

	b.stopChan = make(chan int)
}

// startConsuming is a common part of StopConsuming
func (b *Broker) stopConsuming() {
	// Do not retry from now on
	b.retry = false
	// Notifying the stop channel stops consuming of messages
	b.stopChan <- 1
}
