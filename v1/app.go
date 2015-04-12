package v1

import (
	"encoding/json"
	"runtime"
)

// App is the main Machinery object and stores all configuration
// All the tasks workers process are registered against the app
// App.SendTask is one way of sending a task to workers
type App struct {
	config          *Config
	registeredTasks map[string]Task
	connection      *Connection
}

// InitApp - app constructor
func InitApp(config *Config) *App {
	app := App{
		config:          config,
		registeredTasks: make(map[string]Task),
		connection:      InitConnection(config).Open(),
	}

	runtime.SetFinalizer(&app, func(app *App) {
		app.connection.Close()
	})

	return &app
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
	FailOnError(err, "Could not JSON encode message")
	app.connection.PublishMessage([]byte(message))
}
