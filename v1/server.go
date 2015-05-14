package machinery

import (
	"encoding/json"
	"fmt"

	"github.com/RichardKnop/machinery/v1/config"
)

// Server is the main Machinery object and stores all configuration
// All the tasks workers process are registered against the server
type Server struct {
	config          *config.Config
	registeredTasks map[string]interface{}
	connection      Connectable
}

// NewServer - Server constructor
func NewServer(cnf *config.Config) (*Server, error) {
	var conn Connectable
	var err error

	conn, err = ConnectionFactory(cnf)
	if err != nil {
		return nil, err
	}

	return &Server{
		config:          cnf,
		registeredTasks: make(map[string]interface{}),
		connection:      conn,
	}, nil
}

// NewWorker - Creates a new worker instance
func (server *Server) NewWorker(consumerTag string) *Worker {
	return &Worker{
		server:      server,
		ConsumerTag: consumerTag,
	}
}

// GetConnection returns connection object
func (server *Server) GetConnection() Connectable {
	return server.connection
}

// GetConfig returns connection object
func (server *Server) GetConfig() *config.Config {
	return server.config
}

// RegisterTasks registers all tasks at once
func (server *Server) RegisterTasks(tasks map[string]interface{}) {
	server.registeredTasks = tasks
}

// RegisterTask registers a single task
func (server *Server) RegisterTask(name string, task interface{}) {
	server.registeredTasks[name] = task
}

// GetRegisteredTask returns registered task by name
func (server *Server) GetRegisteredTask(name string) interface{} {
	return server.registeredTasks[name]
}

// SendTask publishes a task to the default queue
func (server *Server) SendTask(s *TaskSignature) error {
	message, err := json.Marshal(s)

	if err != nil {
		return fmt.Errorf("JSON Encode Message: %v", err)
	}

	if err := server.connection.Publish(
		[]byte(message), s.RoutingKey,
	); err != nil {
		return fmt.Errorf("Publish Message: %v", err)
	}

	return nil
}
