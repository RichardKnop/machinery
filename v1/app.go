package machinery

import (
	"encoding/json"
	"fmt"

	"github.com/RichardKnop/machinery/v1/config"
)

// App is the main Machinery object and stores all configuration
// All the tasks workers process are registered against the app
// App.SendTask is one way of sending a task to workers
type App struct {
	config          *config.Config
	registeredTasks map[string]interface{}
	connection      Connectable
}

// InitApp - App constructor
func InitApp(cnf *config.Config) (*App, error) {
	var conn Connectable
	var err error

	conn, err = ConnectionFactory(cnf)
	if err != nil {
		return nil, err
	}

	return &App{
		config:          cnf,
		registeredTasks: make(map[string]interface{}),
		connection:      conn,
	}, nil
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
func (app *App) RegisterTasks(tasks map[string]interface{}) {
	app.registeredTasks = tasks
}

// RegisterTask registers a single task
func (app *App) RegisterTask(name string, task interface{}) {
	app.registeredTasks[name] = task
}

// GetRegisteredTask returns registered task by name
func (app *App) GetRegisteredTask(name string) interface{} {
	return app.registeredTasks[name]
}

// SendTask publishes a task to the default queue
func (app *App) SendTask(s *TaskSignature) error {
	openConn, err := app.connection.Open()
	if err != nil {
		return err
	}

	defer openConn.Close()

	message, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("JSON Encode Message: %v", err)
	}

	err = openConn.PublishMessage([]byte(message), s.RoutingKey)
	if err != nil {
		return fmt.Errorf("Publish Message: %v", err)
	}

	return nil
}
