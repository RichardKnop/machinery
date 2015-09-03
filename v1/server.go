package machinery

import (
	"fmt"
	"sync"

	"github.com/RichardKnop/machinery/Godeps/_workspace/src/code.google.com/p/go-uuid/uuid"
	"github.com/RichardKnop/machinery/v1/backends"
	"github.com/RichardKnop/machinery/v1/brokers"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
)

// Server is the main Machinery object and stores all configuration
// All the tasks workers process are registered against the server
type Server struct {
	config          *config.Config
	registeredTasks map[string]interface{}
	broker          brokers.Broker
	backend         backends.Backend
}

// NewServer creates Server instance
func NewServer(cnf *config.Config) (*Server, error) {
	broker, err := BrokerFactory(cnf)
	if err != nil {
		return nil, err
	}

	// Backend is optional so we ignore the error
	backend, _ := BackendFactory(cnf)

	return &Server{
		config:          cnf,
		registeredTasks: make(map[string]interface{}),
		broker:          broker,
		backend:         backend,
	}, nil
}

// NewWorker creates Worker instance
func (server *Server) NewWorker(consumerTag string) *Worker {
	return &Worker{
		server:      server,
		ConsumerTag: consumerTag,
	}
}

// GetBroker returns broker
func (server *Server) GetBroker() brokers.Broker {
	return server.broker
}

// SetBroker sets broker
func (server *Server) SetBroker(broker brokers.Broker) {
	server.broker = broker
}

// GetBackend returns backend
func (server *Server) GetBackend() backends.Backend {
	return server.backend
}

// SetBackend sets backend
func (server *Server) SetBackend(backend backends.Backend) {
	server.backend = backend
}

// GetConfig returns connection object
func (server *Server) GetConfig() *config.Config {
	return server.config
}

// SetConfig sets config
func (server *Server) SetConfig(cnf *config.Config) {
	server.config = cnf
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
func (server *Server) SendTask(signature *signatures.TaskSignature) (*backends.AsyncResult, error) {
	// Auto generate a UUID if not set already
	if signature.UUID == "" {
		signature.UUID = fmt.Sprintf("task_%v", uuid.New())
	}

	// Set initial task state to PENDING
	if err := server.backend.SetStatePending(signature); err != nil {
		return nil, fmt.Errorf("Set State Pending: %v", err)
	}

	if err := server.broker.Publish(signature); err != nil {
		return nil, fmt.Errorf("Publish Message: %v", err)
	}

	return backends.NewAsyncResult(signature, server.backend), nil
}

// SendChain triggers a chain of tasks
func (server *Server) SendChain(chain *Chain) (*backends.ChainAsyncResult, error) {
	// Set initial task state to PENDING
	if err := server.backend.SetStatePending(chain.Tasks[0]); err != nil {
		return nil, fmt.Errorf("Set State Pending: %v", err)
	}

	if err := server.broker.Publish(chain.Tasks[0]); err != nil {
		return nil, fmt.Errorf("Publish Message: %v", err)
	}

	return backends.NewChainAsyncResult(chain.Tasks, server.backend), nil
}

// SendGroup triggers a group of parallel tasks
func (server *Server) SendGroup(group *Group) ([]*backends.AsyncResult, error) {
	asyncResults := make([]*backends.AsyncResult, len(group.Tasks))

	var wg sync.WaitGroup
	wg.Add(len(group.Tasks))
	errorsChan := make(chan error)

	// Init group
	server.backend.InitGroup(group.GroupUUID, group.GetUUIDs())

	for i, signature := range group.Tasks {
		go func(s *signatures.TaskSignature, index int) {
			defer wg.Done()

			// Set initial task states to PENDING
			if err := server.backend.SetStatePending(s); err != nil {
				errorsChan <- err
				return
			}

			// Publish task
			if err := server.broker.Publish(s); err != nil {
				errorsChan <- fmt.Errorf("Publish Message: %v", err)
				return
			}

			asyncResults[index] = backends.NewAsyncResult(s, server.backend)
		}(signature, i)
	}

	done := make(chan int)
	go func() {
		wg.Wait()
		done <- 1
	}()

	select {
	case err := <-errorsChan:
		return asyncResults, err
	case <-done:
		return asyncResults, nil
	}
}

// SendChord triggers a group of parallel tasks with a callback
func (server *Server) SendChord(chord *Chord) (*backends.ChordAsyncResult, error) {
	_, err := server.SendGroup(chord.Group)
	if err != nil {
		return nil, err
	}

	return backends.NewChordAsyncResult(
		chord.Group.Tasks,
		chord.Callback,
		server.backend,
	), nil
}
