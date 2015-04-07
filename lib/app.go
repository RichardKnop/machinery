package lib

import (
	"fmt"
	"encoding/json"
	"github.com/streadway/amqp"
)

type App struct {
	BrokerURL string
	DefaultTaskQueue string
	registeredTasks []string
}

func InitApp(brokerURL string, defaultTaskQueue string) *App {
	return &App{
		BrokerURL: brokerURL,
		DefaultTaskQueue: defaultTaskQueue,
		registeredTasks: make([]string, 5),
	}
}

func (app *App) SendTask(taskName string) {
	conn, err := amqp.Dial(app.BrokerURL)
	FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		app.DefaultTaskQueue, 	// name
		true,    				// durable
		false,   				// delete when usused
		false,   				// exclusive
		false,   				// no-wait
		nil,     				// arguments
	)
	FailOnError(err, "Failed to declare a queue")

	message := fmt.Sprintf("{\"name\": \"%s\"}", taskName)
	encodedMsgBody, err := json.Marshal(message)
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(encodedMsgBody),
		})
	FailOnError(err, "Failed to publish a message")
}