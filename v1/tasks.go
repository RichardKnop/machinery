package v1

import (
	"encoding/json"

	"github.com/streadway/amqp"
)

// Task is a common interface all registered tasks
// must implement
type Task interface {
	Run(args []interface{}, kwargs map[string]interface{})
}

// TaskMessage is a JSON representation of a task
type TaskMessage struct {
	Name   string
	Args   []interface{}
	Kwargs map[string]interface{}
}

// Send publishes a message to the default queue
func (msg *TaskMessage) Send(c *Connection) {
	c.Open()
	defer c.Conn.Close()
	defer c.Channel.Close()

	encodedMessage, err := json.Marshal(msg)
	FailOnError(err, "Could not JSON encode message")

	err = c.Channel.Publish(
		"",           // exchange
		c.Queue.Name, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(encodedMessage),
		},
	)
	FailOnError(err, "Failed to publish a message")
}
