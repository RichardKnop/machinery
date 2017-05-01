package machinery

import (
	"fmt"

	"github.com/RichardKnop/machinery/v1/backends"
	"github.com/RichardKnop/machinery/v1/logger"
	"github.com/RichardKnop/machinery/v1/signatures"
)

// Worker represents a single worker process
type Worker struct {
	server      *Server
	ConsumerTag string
}

// Launch starts a new worker process. The worker subscribes
// to the default queue and processes incoming registered tasks
func (worker *Worker) Launch() error {
	cnf := worker.server.GetConfig()
	broker := worker.server.GetBroker()

	logger.Get().Printf("Launching a worker with the following settings:")
	logger.Get().Printf("- Broker: %s", cnf.Broker)
	logger.Get().Printf("- DefaultQueue: %s", cnf.DefaultQueue)
	logger.Get().Printf("- ResultBackend: %s", cnf.ResultBackend)
	if cnf.AMQP != nil {
		logger.Get().Printf("- AMQP: %s", cnf.AMQP.Exchange)
		logger.Get().Printf("  - Exchange: %s", cnf.AMQP.Exchange)
		logger.Get().Printf("  - ExchangeType: %s", cnf.AMQP.ExchangeType)
		logger.Get().Printf("  - BindingKey: %s", cnf.AMQP.BindingKey)
		logger.Get().Printf("  - PrefetchCount: %d", cnf.AMQP.PrefetchCount)
	}

	errorsChan := make(chan error)

	go func() {
		for {
			retry, err := broker.StartConsuming(worker.ConsumerTag, worker)

			if retry {
				logger.Get().Printf("Going to retry launching the worker. Error: %v", err)
			} else {
				errorsChan <- err // stop the goroutine
				return
			}
		}
	}()

	return <-errorsChan
}

// Quit tears down the running worker process
func (worker *Worker) Quit() {
	worker.server.GetBroker().StopConsuming()
}

// Process handles received tasks and triggers success/error callbacks
func (worker *Worker) Process(signature *signatures.TaskSignature) error {
	// If the task is not registered with this worker, do not continue
	// but only return nil as we do not want to restart the worker process
	if !worker.server.IsTaskRegistered(signature.Name) {
		return nil
	}

	taskFunc, err := worker.server.GetRegisteredTask(signature.Name)
	if err != nil {
		return nil
	}

	backend := worker.server.GetBackend()

	// Update task state to RECEIVED
	if err = backend.SetStateReceived(signature); err != nil {
		return fmt.Errorf("Set State Received: %v", err)
	}

	// Prepare task for processing
	task, err := NewTask(taskFunc, signature.Args)
	if err != nil {
		worker.finalizeError(signature, err)
		return err
	}

	// Update task state to STARTED
	if err = backend.SetStateStarted(signature); err != nil {
		return fmt.Errorf("Set State Started: %v", err)
	}

	// Call the task
	results, err := task.Call()
	if err != nil {
		return worker.finalizeError(signature, err)
	}

	return worker.finalizeSuccess(signature, results)
}

// Task succeeded, update state and trigger success callbacks
func (worker *Worker) finalizeSuccess(signature *signatures.TaskSignature, taskResults []*backends.TaskResult) error {
	// Update task state to SUCCESS
	backend := worker.server.GetBackend()

	if err := backend.SetStateSuccess(signature, taskResults); err != nil {
		return fmt.Errorf("Set State Success: %v", err)
	}

	logger.Get().Printf("Processed %s. Results = %v", signature.UUID, taskResults)

	// Trigger success callbacks
	for _, successTask := range signature.OnSuccess {
		if signature.Immutable == false {
			// Pass results of the task to success callbacks
			args := make([]signatures.TaskArg, 0)
			for _, taskResult := range taskResults {
				args = append([]signatures.TaskArg{{
					Type:  taskResult.Type,
					Value: taskResult.Value,
				}}, successTask.Args...)
			}
			successTask.Args = args
		}

		worker.server.SendTask(successTask)
	}

	// If the task was not part of a group, just return
	if signature.GroupUUID == "" {
		return nil
	}

	// Check if all task in the group has completed
	groupCompleted, err := worker.server.GetBackend().GroupCompleted(
		signature.GroupUUID,
		signature.GroupTaskCount,
	)
	if err != nil {
		return fmt.Errorf("GroupCompleted: %v", err)
	}
	// If the group has not yet completed, just return
	if !groupCompleted {
		return nil
	}

	// Defer purging of group meta queue if we are using AMQP backend
	if worker.hasAMQPBackend() {
		defer worker.server.backend.PurgeGroupMeta(signature.GroupUUID)
	}

	// There is no chord callback, just return
	if signature.ChordCallback == nil {
		return nil
	}

	// Trigger chord callback
	shouldTrigger, err := worker.server.backend.TriggerChord(signature.GroupUUID)
	if err != nil {
		return fmt.Errorf("TriggerChord: %v", err)
	}

	// Chord has already been triggered
	if !shouldTrigger {
		return nil
	}

	// Get task states
	taskStates, err := worker.server.GetBackend().GroupTaskStates(
		signature.GroupUUID,
		signature.GroupTaskCount,
	)
	if err != nil {
		return nil
	}

	// Append group tasks' return values to chord task if it's not immutable
	for _, taskState := range taskStates {
		if !taskState.IsSuccess() {
			return nil
		}

		if signature.ChordCallback.Immutable == false {
			// Pass results of the task to the chord callback
			for _, taskResult := range taskState.Results {
				signature.ChordCallback.Args = append(signature.ChordCallback.Args, signatures.TaskArg{
					Type:  taskResult.Type,
					Value: taskResult.Value,
				})
			}
		}
	}

	// Send the chord task
	_, err = worker.server.SendTask(signature.ChordCallback)
	if err != nil {
		return err
	}

	return nil
}

// Task failed, update state and trigger error callbacks
func (worker *Worker) finalizeError(signature *signatures.TaskSignature, err error) error {
	// Update task state to FAILURE
	backend := worker.server.GetBackend()
	if err1 := backend.SetStateFailure(signature, err.Error()); err1 != nil {
		return fmt.Errorf("Set State Failure: %v", err1)
	}

	logger.Get().Printf("Failed processing %s. Error = %v", signature.UUID, err)

	// Trigger error callbacks
	for _, errorTask := range signature.OnError {
		// Pass error as a first argument to error callbacks
		args := append([]signatures.TaskArg{{
			Type:  "string",
			Value: err.Error(),
		}}, errorTask.Args...)
		errorTask.Args = args
		worker.server.SendTask(errorTask)
	}

	return nil
}

// Returns true if the worker uses AMQP backend
func (worker *Worker) hasAMQPBackend() bool {
	_, ok := worker.server.backend.(*backends.AMQPBackend)
	return ok
}
