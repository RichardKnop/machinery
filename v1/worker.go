package machinery

import (
	"errors"
	"fmt"
	"log"
	"reflect"

	"github.com/RichardKnop/machinery/v1/backends"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/RichardKnop/machinery/v1/utils"
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

	log.Printf("Launching a worker with the following settings:")
	log.Printf("- Broker: %s", cnf.Broker)
	log.Printf("- ResultBackend: %s", cnf.ResultBackend)
	log.Printf("- Exchange: %s", cnf.Exchange)
	log.Printf("- ExchangeType: %s", cnf.ExchangeType)
	log.Printf("- DefaultQueue: %s", cnf.DefaultQueue)
	log.Printf("- BindingKey: %s", cnf.BindingKey)

	errorsChan := make(chan error)

	go func() {
		for {
			retry, err := broker.StartConsuming(worker.ConsumerTag, worker)

			if retry {
				log.Printf("Going to retry launching the worker. Error: %v", err)
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

	task, err := worker.server.GetRegisteredTask(signature.Name)
	if err != nil {
		return nil
	}

	backend := worker.server.GetBackend()
	// Update task state to RECEIVED
	if err := backend.SetStateReceived(signature); err != nil {
		return fmt.Errorf("Set State Received: %v", err)
	}

	// Get task args and reflect them to proper types
	reflectedTask := reflect.ValueOf(task)
	reflectedArgs, err := reflectArgs(signature.Args)
	if err != nil {
		worker.finalizeError(signature, err)
		return fmt.Errorf("Reflect task args: %v", err)
	}

	// Update task state to STARTED
	if err := backend.SetStateStarted(signature); err != nil {
		return fmt.Errorf("Set State Started: %v", err)
	}

	// Call the task passing in the correct arguments
	results, err := tryCall(reflectedTask, reflectedArgs)

	if err != nil {
		return worker.finalizeError(signature, err)
	}

	return worker.finalizeSuccess(signature, results[0])
}

// Converts []TaskArg to []reflect.Value
func reflectArgs(args []signatures.TaskArg) ([]reflect.Value, error) {
	argValues := make([]reflect.Value, len(args))

	for i, arg := range args {
		argValue, err := utils.ReflectValue(arg.Type, arg.Value)
		if err != nil {
			return nil, err
		}
		argValues[i] = argValue
	}

	return argValues, nil
}

// Attempts to call the task with the supplied arguments.
//
// `err` is set in the return value in two cases:
// 1. The reflected function invocation panics (e.g. due to a mismatched
//    argument list).
// 2. The task func itself returns a non-nil error.
func tryCall(f reflect.Value, args []reflect.Value) (results []reflect.Value, err error) {
	defer func() {
		// Recover from panic and set err.
		if e := recover(); e != nil {
			switch e := e.(type) {
			default:
				err = errors.New("Invoking task caused a panic")
			case error:
				err = e
			case string:
				err = errors.New(e)
			}
		}
	}()

	results = f.Call(args)

	// If an error was returned by the task func, propagate it
	// to the caller via err.
	if !results[1].IsNil() {
		return nil, results[1].Interface().(error)
	}

	return results, err
}

func createTaskResult(value reflect.Value) *backends.TaskResult {
	return &backends.TaskResult{
		Type:  reflect.TypeOf(value.Interface()).String(),
		Value: value.Interface(),
	}
}

// Task succeeded, update state and trigger success callbacks
func (worker *Worker) finalizeSuccess(signature *signatures.TaskSignature, result reflect.Value) error {
	// Update task state to SUCCESS
	backend := worker.server.GetBackend()

	taskResult := createTaskResult(result)
	if err := backend.SetStateSuccess(signature, taskResult); err != nil {
		return fmt.Errorf("Set State Success: %v", err)
	}

	log.Printf("Processed %s. Result = %v", signature.UUID, taskResult.Value)

	// Trigger success callbacks
	for _, successTask := range signature.OnSuccess {
		if signature.Immutable == false {
			// Pass results of the task to success callbacks
			args := append([]signatures.TaskArg{signatures.TaskArg{
				Type:  taskResult.Type,
				Value: taskResult.Value,
			}}, successTask.Args...)
			successTask.Args = args
		}

		worker.server.SendTask(successTask)
	}

	if signature.GroupUUID != "" {
		groupCompleted, err := worker.server.GetBackend().GroupCompleted(
			signature.GroupUUID,
			signature.GroupTaskCount,
		)
		if err != nil {
			return fmt.Errorf("GroupCompleted: %v", err)
		}
		if !groupCompleted {
			return nil
		}

		// Optionally trigger chord callback
		if signature.ChordCallback != nil {
			taskStates, err := worker.server.GetBackend().GroupTaskStates(
				signature.GroupUUID,
				signature.GroupTaskCount,
			)
			if err != nil {
				return nil
			}

			for _, taskState := range taskStates {
				if !taskState.IsSuccess() {
					return nil
				}

				if signature.ChordCallback.Immutable == false {
					// Pass results of the task to the chord callback
					signature.ChordCallback.Args = append(signature.ChordCallback.Args, signatures.TaskArg{
						Type:  taskState.Result.Type,
						Value: taskState.Result.Value,
					})
				}
			}

			_, err = worker.server.SendTask(signature.ChordCallback)
			if err != nil {
				return err
			}
		}

		// Purge group state if we are using AMQP backend and all tasks finished
		if worker.hasAMQPBackend() {
			err = worker.server.backend.PurgeGroupMeta(signature.GroupUUID)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Task failed, update state and trigger error callbacks
func (worker *Worker) finalizeError(signature *signatures.TaskSignature, err error) error {
	// Update task state to FAILURE
	backend := worker.server.GetBackend()
	if err := backend.SetStateFailure(signature, err.Error()); err != nil {
		return fmt.Errorf("Set State Failure: %v", err)
	}

	log.Printf("Failed processing %s. Error = %v", signature.UUID, err)

	// Trigger error callbacks
	for _, errorTask := range signature.OnError {
		// Pass error as a first argument to error callbacks
		args := append([]signatures.TaskArg{signatures.TaskArg{
			Type:  reflect.TypeOf(err).String(),
			Value: reflect.ValueOf(err).Interface(),
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
