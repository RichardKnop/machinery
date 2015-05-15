package brokers

import (
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/streadway/amqp"
)

// Broker - a common interface for all brokers
type Broker interface {
	Consume(consumerTag string, p MessageProcessor) error
	Publish(task *signatures.TaskSignature) error
}

// MessageProcessor - can process a delivered message
// This will probably always be a worker instance
type MessageProcessor interface {
	ProcessMessage(d *amqp.Delivery)
}
