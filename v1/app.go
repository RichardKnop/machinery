package machinery

import (
	"encoding/json"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/errors"
)

// App is the main Machinery object and stores all configuration
// All the tasks workers process are registered against the app
// App.SendTask is one way of sending a task to workers
type App struct {
	config          *config.Config
	registeredTasks map[string]Task
	connection      Connectable
}

// InitApp - App constructor
func InitApp(cnf *config.Config) *App {
	var conn Connectable
	var err error

	conn, err = ConnectionFactory(cnf)
	if err != nil {
		errors.Fail(err, "Failed to create a connection")
	}

	return &App{
		config:          cnf,
		registeredTasks: make(map[string]Task),
		connection:      conn.Open(),
	}
}

// GetConnection returns connection object
func (app *App) GetConnection() Connectable {
	return app.connection
}

// GetConfig returns connection object
func (app *App) GetConfig() *config.Config {
	return app.config
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

// SendTask publishes a task to the default queue
func (app *App) SendTask(s *TaskSignature) {
	message, err := json.Marshal(s)
	errors.Fail(err, "Failed to JSON encode message")
	app.connection.PublishMessage([]byte(message))
}
