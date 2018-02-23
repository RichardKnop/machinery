package brokers

import (
	"errors"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/retry"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// Broker represents a base broker structure
type Broker struct {
	cnf                 *config.Config
	registeredTaskNames []string
	retry               bool
	retryFunc           func(chan int)
	retryStopChan       chan int
	stopChan            chan int
}

// New creates new Broker instance
func New(cnf *config.Config) Broker {
	return Broker{cnf: cnf, retry: true}
}

// GetConfig returns config
func (b *Broker) GetConfig() *config.Config {
	return b.cnf
}

// StartConsuming enters a loop and waits for incoming messages
func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor TaskProcessor) (bool, error) {
	return false, errors.New("Not implemented")
}

// Publish places a new message on the default queue
func (b *Broker) Publish(signature *tasks.Signature) error {
	return errors.New("Not implemented")
}

// StopConsuming quits the loop
func (b *Broker) StopConsuming() {
	//
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
	b.retryStopChan = make(chan int)
}

// startConsuming is a common part of StopConsuming
func (b *Broker) stopConsuming() {
	// Do not retry from now on
	b.retry = false
	// Stop the retry closure earlier
	select {
	case b.retryStopChan <- 1:
		log.WARNING.Print("Stopping retry closure.")
	default:
	}
	// Notifying the stop channel stops consuming of messages
	b.stopChan <- 1
}

// GetRegisteredTaskNames returns registered tasks names
func (b *Broker) GetRegisteredTaskNames() []string {
	return b.registeredTaskNames
}

// AdjustRoutingKey makes sure the routing key is correct.
// If the routing key is an empty string:
// a) set it to binding key for direct exchange type
// b) set it to default queue name
func AdjustRoutingKey(b Interface, s *tasks.Signature) {
	if s.RoutingKey != "" {
		return
	}

	if IsAMQP(b) && b.GetConfig().AMQP != nil && b.GetConfig().AMQP.ExchangeType == "direct" {
		// The routing algorithm behind a direct exchange is simple - a message goes
		// to the queues whose binding key exactly matches the routing key of the message.
		s.RoutingKey = b.GetConfig().AMQP.BindingKey
		return
	}

	s.RoutingKey = b.GetConfig().DefaultQueue
}

// IsAMQP returns true if the broker is AMQP
func IsAMQP(b Interface) bool {
	_, isAMQPBroker := b.(*AMQPBroker)
	return isAMQPBroker
}
