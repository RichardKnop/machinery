package v1

import (
	"encoding/json"

	"github.com/streadway/amqp"
)

// App is the main MAchinery object and stores all configuration
// All the tasks workers process are registered against the app
// App.SendTask is one way of sending a task to workers
type App struct {
	Config          *Config
	registeredTasks map[string]Task
}

// InitApp - app constructor
func InitApp(config *Config) *App {
	return &App{
		Config: config,
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

// SendTask sends a task to the default queue
func (app *App) SendTask(name string, kwargs map[string]interface{}) {
	conn, ch, q := Connect(app)
	defer conn.Close()
	defer ch.Close()

	message := make(map[string]interface{})
	message["name"] = name
	message["kwargs"] = kwargs
	encodedMessage, err := json.Marshal(message)

	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(encodedMessage),
		},
	)
	FailOnError(err, "Failed to publish a message")
}
