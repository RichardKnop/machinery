package v1

import (
	"encoding/json"

	"github.com/streadway/amqp"
)

// App is the main MAchinery object and stores all configuration
// All the tasks workers process are registered against the app
// App.SendTask is one way of sending a task to workers
type App struct {
	config          *Config
	registeredTasks map[string]Task
}

// InitApp - app constructor
func InitApp(config *Config) *App {
	return &App{
		config: config,
	}
}

// RegisterTasks registers all tasks at once
func (app *App) RegisterTasks(tasks map[string]Task) {
	app.registeredTasks = tasks
}

// RegisterTask registers a single task
func (app *App) RegisterTask(name string, task Task) {
	app.registeredTasks[name] = task
}

// GetRegisteredTask returns registered task by name
func (app *App) GetRegisteredTask(name string) Task {
	return app.registeredTasks[name]
}

// NewConnection creates a new broker connection
func (app *App) NewConnection() *Connection {
	return InitConnection(app.config)
}

// SendTask publishes a task to the default queue
func (app *App) SendTask(
	name string,
	args []interface{},
	kwargs map[string]interface{},
) {
	msg := TaskMessage{
		Name:   name,
		Args:   args,
		Kwargs: kwargs,
	}
	encodedMessage, err := json.Marshal(msg)
	FailOnError(err, "Could not JSON encode message")

	c := app.NewConnection().Open()
	defer c.Conn.Close()
	defer c.Channel.Close()

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
