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

	errChan := make(chan error)

	go func() {
		retryFunc := utils.RetryClosure()
		for {
			retryFunc()
			retry, err := broker.StartConsuming(worker.ConsumerTag, worker)

			if !retry {
				errChan <- err // stop the goroutine
				break
			}

			log.Print(err)
		}
	}()

	return <-errChan
}

// Quit tears down the running worker process
func (worker *Worker) Quit() {
	worker.server.GetBroker().StopConsuming()
}

// Process handles received tasks and triggers success/error callbacks
func (worker *Worker) Process(signature *signatures.TaskSignature) error {
	task := worker.server.GetRegisteredTask(signature.Name)
	if task == nil {
		return fmt.Errorf("Task not registered: %v", signature.Name)
	}

	backend := worker.server.GetBackend()
	// Update task state to RECEIVED
	if err := backend.SetStateReceived(signature); err != nil {
		return fmt.Errorf("Set State Received: %v", err)
	}

	// Get task args and reflect them to proper types
	reflectedTask := reflect.ValueOf(task)
	relfectedArgs, err := worker.reflectArgs(signature.Args)
	if err != nil {
		return fmt.Errorf("Reflect task args: %v", err)
	}

	// Update task state to STARTED
	if err := backend.SetStateStarted(signature); err != nil {
		return fmt.Errorf("Set State Started: %v", err)
	}

	// Call the task passing in the correct arguments
	results := reflectedTask.Call(relfectedArgs)
	if !results[1].IsNil() {
		return worker.finalizeError(signature, errors.New(results[1].String()))
	}

	return worker.finalizeSuccess(signature, results[0])
}

// Converts []TaskArg to []reflect.Value
func (worker *Worker) reflectArgs(args []signatures.TaskArg) ([]reflect.Value, error) {
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

// Task succeeded, update state and trigger success callbacks
func (worker *Worker) finalizeSuccess(signature *signatures.TaskSignature, result reflect.Value) error {
	// Update task state to SUCCESS
	backend := worker.server.GetBackend()
	taskResult := &backends.TaskResult{
		Type:  result.Type().String(),
		Value: result.Interface(),
	}
	if err := backend.SetStateSuccess(signature, taskResult); err != nil {
		return fmt.Errorf("Set State Success: %v", err)
	}

	log.Printf("Processed %s. Result = %v", signature.UUID, result.Interface())

	for _, successTask := range signature.OnSuccess {
		if signature.Immutable == false {
			// Pass results of the task to success callbacks
			args := append([]signatures.TaskArg{signatures.TaskArg{
				Type:  result.Type().String(),
				Value: result.Interface(),
			}}, successTask.Args...)
			successTask.Args = args
		}

		worker.server.SendTask(successTask)
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
