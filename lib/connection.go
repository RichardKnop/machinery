package lib

import (
	"github.com/streadway/amqp"
)

func Connect(app *App) (*amqp.Connection, *amqp.Channel, amqp.Queue) {
	conn, err := amqp.Dial(app.BrokerURL)
	FailOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	FailOnError(err, "Failed to open a channel")

	q, err := ch.QueueDeclare(
		app.DefaultQueue, 	// name
		true,         		// durable
		false,        		// delete when unused
		false,        		// exclusive
		false,        		// no-wait
		nil,          		// arguments
	)
	FailOnError(err, "Failed to declare a queue")

	return conn, ch, q
}