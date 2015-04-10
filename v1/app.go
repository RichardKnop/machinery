package v1

import "encoding/json"

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
func (app *App) SendTask(signature *TaskSignature) {
	message, err := json.Marshal(signature)
	FailOnError(err, "Could not JSON encode message")

	c := app.NewConnection().Open()
	c.PublishMessage([]byte(message))
	c.Conn.Close()
	c.Channel.Close()
}
