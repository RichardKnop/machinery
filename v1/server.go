package machinery

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/RichardKnop/machinery/v1/backends/result"
	"github.com/RichardKnop/machinery/v1/brokers/eager"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"

	backendsiface "github.com/RichardKnop/machinery/v1/backends/iface"
	brokersiface "github.com/RichardKnop/machinery/v1/brokers/iface"
)

// Server is the main Machinery object and stores all configuration
// All the tasks workers process are registered against the server
type Server struct {
	config                 *config.Config
	registeredTasks        map[string]interface{}
	broker                 brokersiface.Broker
	backend                backendsiface.Backend
	prePublishHandler      func(context.Context, *tasks.Signature) func()
	preChainPublishHandler func(context.Context, *tasks.Chain) func()
	preChordPublishHandler func(context.Context, *tasks.Chord, int) func()
	preGroupPublishHandler func(context.Context, *tasks.Group, int) func()
}

// NewServerWithBrokerBackend ...
func NewServerWithBrokerBackend(cnf *config.Config, brokerServer brokersiface.Broker, backendServer backendsiface.Backend) *Server {
	return &Server{
		config:                 cnf,
		registeredTasks:        make(map[string]interface{}),
		broker:                 brokerServer,
		backend:                backendServer,
		prePublishHandler:      defaultPrePublishHandler,
		preChainPublishHandler: defaultPreChainPublishHandler,
		preChordPublishHandler: defaultPreChordPublishHandler,
		preGroupPublishHandler: defaultPreGroupPublishHandler,
	}
}

// NewServer creates Server instance
func NewServer(cnf *config.Config) (*Server, error) {
	broker, err := BrokerFactory(cnf)
	if err != nil {
		return nil, err
	}

	// Backend is optional so we ignore the error
	backend, _ := BackendFactory(cnf)

	srv := NewServerWithBrokerBackend(cnf, broker, backend)

	// init for eager-mode
	eager, ok := broker.(eager.Mode)
	if ok {
		// we don't have to call worker.Launch in eager mode
		eager.AssignWorker(srv.NewWorker("eager", 0))
	}

	return srv, nil
}

// NewWorker creates Worker instance
func (server *Server) NewWorker(consumerTag string, concurrency int) *Worker {
	return &Worker{
		server:          server,
		ConsumerTag:     consumerTag,
		Concurrency:     concurrency,
		Queue:           "",
		preTaskHandler:  defaultPreTaskHandler,
		postTaskHandler: defaultPostTaskHandler,
	}
}

// NewCustomQueueWorker creates Worker instance with Custom Queue
func (server *Server) NewCustomQueueWorker(consumerTag string, concurrency int, queue string) *Worker {
	return &Worker{
		server:          server,
		ConsumerTag:     consumerTag,
		Concurrency:     concurrency,
		Queue:           queue,
		preTaskHandler:  defaultPreTaskHandler,
		postTaskHandler: defaultPostTaskHandler,
	}
}

// GetBroker returns broker
func (server *Server) GetBroker() brokersiface.Broker {
	return server.broker
}

// SetBroker sets broker
func (server *Server) SetBroker(broker brokersiface.Broker) {
	server.broker = broker
}

// GetBackend returns backend
func (server *Server) GetBackend() backendsiface.Backend {
	return server.backend
}

// SetBackend sets backend
func (server *Server) SetBackend(backend backendsiface.Backend) {
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

// SetPreTaskHandler Sets pre publish handler
func (server *Server) SetPreTaskHandler(handler func(*tasks.Signature)) {
	server.prePublishHandler = func(ctx context.Context, signature *tasks.Signature) func() {
		handler(signature)
		// preserve opentracing behavior
		return defaultPrePublishHandler(ctx, signature)
	}
}

// SetPreTaskContextHandler Sets pre publish handler using a task context
func (server *Server) SetPreTaskContextHandler(handler func(context.Context, *tasks.Signature) func()) {
	server.prePublishHandler = func(ctx context.Context, signature *tasks.Signature) func() {
		// preserve opentracing behavior
		f1 := defaultPrePublishHandler(ctx, signature)
		f2 := handler(ctx, signature)

		return finishFunc(f1, f2)
	}
}

// SetPreChainContextHandler Sets pre publish handler using a task context
func (server *Server) SetPreChainContextHandler(handler func(context.Context, *tasks.Chain) func()) {
	server.preChainPublishHandler = func(ctx context.Context, chain *tasks.Chain) func() {
		// preserve opentracing behavior
		f1 := defaultPreChainPublishHandler(ctx, chain)
		f2 := handler(ctx, chain)

		return finishFunc(f1, f2)
	}
}

// SetPreGroupContextHandler Sets pre publish handler using a task context
func (server *Server) SetPreGroupContextHandler(handler func(context.Context, *tasks.Group, int) func()) {
	server.preGroupPublishHandler = func(ctx context.Context, group *tasks.Group, sendConcurrency int) func() {
		// preserve opentracing behavior
		f1 := defaultPreGroupPublishHandler(ctx, group, sendConcurrency)
		f2 := handler(ctx, group, sendConcurrency)

		return finishFunc(f1, f2)
	}
}

// SetPreTaskContextHandler Sets pre publish handler using a task context
func (server *Server) SetPreChordContextHandler(handler func(context.Context, *tasks.Chord, int) func()) {
	server.preChordPublishHandler = func(ctx context.Context, chord *tasks.Chord, sendConcurrency int) func() {
		// preserve opentracing behavior
		f1 := defaultPreChordPublishHandler(ctx, chord, sendConcurrency)
		f2 := handler(ctx, chord, sendConcurrency)

		return finishFunc(f1, f2)
	}
}

// RegisterTasks registers all tasks at once
func (server *Server) RegisterTasks(namedTaskFuncs map[string]interface{}) error {
	for _, task := range namedTaskFuncs {
		if err := tasks.ValidateTask(task); err != nil {
			return err
		}
	}
	server.registeredTasks = namedTaskFuncs
	server.broker.SetRegisteredTaskNames(server.GetRegisteredTaskNames())
	return nil
}

// RegisterTask registers a single task
func (server *Server) RegisterTask(name string, taskFunc interface{}) error {
	if err := tasks.ValidateTask(taskFunc); err != nil {
		return err
	}
	server.registeredTasks[name] = taskFunc
	server.broker.SetRegisteredTaskNames(server.GetRegisteredTaskNames())
	return nil
}

// IsTaskRegistered returns true if the task name is registered with this broker
func (server *Server) IsTaskRegistered(name string) bool {
	_, ok := server.registeredTasks[name]
	return ok
}

// GetRegisteredTask returns registered task by name
func (server *Server) GetRegisteredTask(name string) (interface{}, error) {
	taskFunc, ok := server.registeredTasks[name]
	if !ok {
		return nil, fmt.Errorf("Task not registered error: %s", name)
	}
	return taskFunc, nil
}

// SendTaskWithContext will inject the trace context in the signature headers before publishing it
func (server *Server) SendTaskWithContext(ctx context.Context, signature *tasks.Signature) (*result.AsyncResult, error) {

	if server.prePublishHandler != nil {
		finish := server.prePublishHandler(ctx, signature)
		defer finish()
	}

	// Make sure result backend is defined
	if server.backend == nil {
		return nil, errors.New("Result backend required")
	}

	// Auto generate a UUID if not set already
	if signature.UUID == "" {
		taskID := uuid.New().String()
		signature.UUID = fmt.Sprintf("task_%v", taskID)
	}

	// Set initial task state to PENDING
	if err := server.backend.SetStatePending(signature); err != nil {
		return nil, fmt.Errorf("Set state pending error: %s", err)
	}

	if err := server.broker.Publish(ctx, signature); err != nil {
		return nil, fmt.Errorf("Publish message error: %s", err)
	}

	return result.NewAsyncResult(signature, server.backend), nil
}

// SendTask publishes a task to the default queue
func (server *Server) SendTask(signature *tasks.Signature) (*result.AsyncResult, error) {
	return server.SendTaskWithContext(context.Background(), signature)
}

// SendChainWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendChainWithContext(ctx context.Context, chain *tasks.Chain) (*result.ChainAsyncResult, error) {

	if server.preChainPublishHandler != nil {
		finish := server.preChainPublishHandler(ctx, chain)
		defer finish()
	}

	return server.SendChain(chain)
}

// SendChain triggers a chain of tasks
func (server *Server) SendChain(chain *tasks.Chain) (*result.ChainAsyncResult, error) {
	_, err := server.SendTask(chain.Tasks[0])
	if err != nil {
		return nil, err
	}

	return result.NewChainAsyncResult(chain.Tasks, server.backend), nil
}

// SendGroupWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendGroupWithContext(ctx context.Context, group *tasks.Group, sendConcurrency int) ([]*result.AsyncResult, error) {
	if server.preGroupPublishHandler != nil {
		finish := server.preGroupPublishHandler(ctx, group, sendConcurrency)
		defer finish()
	}

	// Make sure result backend is defined
	if server.backend == nil {
		return nil, errors.New("Result backend required")
	}

	asyncResults := make([]*result.AsyncResult, len(group.Tasks))

	var wg sync.WaitGroup
	wg.Add(len(group.Tasks))
	errorsChan := make(chan error, len(group.Tasks)*2)

	// Init group
	server.backend.InitGroup(group.GroupUUID, group.GetUUIDs())

	// Init the tasks Pending state first
	for _, signature := range group.Tasks {
		if err := server.backend.SetStatePending(signature); err != nil {
			errorsChan <- err
			continue
		}
	}

	pool := make(chan struct{}, sendConcurrency)
	go func() {
		for i := 0; i < sendConcurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for i, signature := range group.Tasks {

		if sendConcurrency > 0 {
			<-pool
		}

		go func(s *tasks.Signature, index int) {
			defer wg.Done()

			// Publish task

			err := server.broker.Publish(ctx, s)

			if sendConcurrency > 0 {
				pool <- struct{}{}
			}

			if err != nil {
				errorsChan <- fmt.Errorf("Publish message error: %s", err)
				return
			}

			asyncResults[index] = result.NewAsyncResult(s, server.backend)
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

// SendGroup triggers a group of parallel tasks
func (server *Server) SendGroup(group *tasks.Group, sendConcurrency int) ([]*result.AsyncResult, error) {
	return server.SendGroupWithContext(context.Background(), group, sendConcurrency)
}

// SendChordWithContext will inject the trace context in all the signature headers before publishing it
func (server *Server) SendChordWithContext(ctx context.Context, chord *tasks.Chord, sendConcurrency int) (*result.ChordAsyncResult, error) {

	if server.preGroupPublishHandler != nil {
		finish := server.preChordPublishHandler(ctx, chord, sendConcurrency)
		defer finish()
	}

	_, err := server.SendGroupWithContext(ctx, chord.Group, sendConcurrency)
	if err != nil {
		return nil, err
	}

	return result.NewChordAsyncResult(
		chord.Group.Tasks,
		chord.Callback,
		server.backend,
	), nil
}

// SendChord triggers a group of parallel tasks with a callback
func (server *Server) SendChord(chord *tasks.Chord, sendConcurrency int) (*result.ChordAsyncResult, error) {
	return server.SendChordWithContext(context.Background(), chord, sendConcurrency)
}

// GetRegisteredTaskNames returns slice of registered task names
func (server *Server) GetRegisteredTaskNames() []string {
	taskNames := make([]string, len(server.registeredTasks))
	var i = 0
	for name := range server.registeredTasks {
		taskNames[i] = name
		i++
	}
	return taskNames
}
