package common

import (
	"errors"
	"sync"

	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/retry"
	"github.com/RichardKnop/machinery/v1/tasks"
)

type registeredTaskNames struct {
	sync.RWMutex
	items []string
}

// Broker represents a base broker structure
type Broker struct {
	cnf                 *config.Config
	registeredTaskNames registeredTaskNames
	retry               bool
	retryFunc           func(chan int)
	retryStopChan       chan int
	stopChan            chan int
}

// NewBroker creates new Broker instance
func NewBroker(cnf *config.Config) Broker {
	return Broker{
		cnf:           cnf,
		retry:         true,
		stopChan:      make(chan int),
		retryStopChan: make(chan int),
	}
}

// GetConfig returns config
func (b *Broker) GetConfig() *config.Config {
	return b.cnf
}

// GetRetry ...
func (b *Broker) GetRetry() bool {
	return b.retry
}

// GetRetryFunc ...
func (b *Broker) GetRetryFunc() func(chan int) {
	return b.retryFunc
}

// GetRetryStopChan ...
func (b *Broker) GetRetryStopChan() chan int {
	return b.retryStopChan
}

// GetStopChan ...
func (b *Broker) GetStopChan() chan int {
	return b.stopChan
}

// Publish places a new message on the default queue
func (b *Broker) Publish(signature *tasks.Signature) error {
	return errors.New("Not implemented")
}

// SetRegisteredTaskNames sets registered task names
func (b *Broker) SetRegisteredTaskNames(names []string) {
	b.registeredTaskNames.Lock()
	defer b.registeredTaskNames.Unlock()
	b.registeredTaskNames.items = names
}

// IsTaskRegistered returns true if the task is registered with this broker
func (b *Broker) IsTaskRegistered(name string) bool {
	b.registeredTaskNames.RLock()
	defer b.registeredTaskNames.RUnlock()
	for _, registeredTaskName := range b.registeredTaskNames.items {
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

// GetDelayedTasks returns a slice of task.Signatures that are scheduled, but not yet in the queue
func (b *Broker) GetDelayedTasks() ([]*tasks.Signature, error) {
	return nil, errors.New("Not implemented")
}

// StartConsuming is a common part of StartConsuming method
func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) {
	if b.retryFunc == nil {
		b.retryFunc = retry.Closure()
	}

}

// StopConsuming is a common part of StopConsuming
func (b *Broker) StopConsuming() {
	// Do not retry from now on
	b.retry = false
	// Stop the retry closure earlier
	select {
	case b.retryStopChan <- 1:
		log.WARNING.Print("Stopping retry closure.")
	default:
	}
	// Notifying the stop channel stops consuming of messages
	close(b.stopChan)
	log.WARNING.Print("Stop channel")
}

// GetRegisteredTaskNames returns registered tasks names
func (b *Broker) GetRegisteredTaskNames() []string {
	b.registeredTaskNames.RLock()
	defer b.registeredTaskNames.RUnlock()
	items := b.registeredTaskNames.items
	return items
}

// AdjustRoutingKey makes sure the routing key is correct.
// If the routing key is an empty string:
// a) set it to binding key for direct exchange type
// b) set it to default queue name
func (b *Broker) AdjustRoutingKey(s *tasks.Signature) {
	if s.RoutingKey != "" {
		return
	}

	s.RoutingKey = b.GetConfig().DefaultQueue
}
