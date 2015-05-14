package brokers

import "github.com/streadway/amqp"

// Broker - a common interface for all brokers
type Broker interface {
	Consume(consumerTag string, p MessageProcessor) error
	Publish(body []byte, routingKey string) error
}

// MessageProcessor - can process a delivered message
// This will probably always be a worker instance
type MessageProcessor interface {
	ProcessMessage(d *amqp.Delivery)
}
