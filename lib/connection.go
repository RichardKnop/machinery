package lib

import (
	"github.com/streadway/amqp"
)

// Connect onnects to the message queue, opens a channel,
// declares a queue and returns connection, channel
// and queue objects
func Connect(app *App) (*amqp.Connection, *amqp.Channel, amqp.Queue) {
	conn, err := amqp.Dial(app.Config.BrokerURL)
	FailOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	FailOnError(err, "Failed to open a channel")

	q, err := ch.QueueDeclare(
		app.Config.DefaultQueue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	FailOnError(err, "Failed to declare a queue")

	return conn, ch, q
}
