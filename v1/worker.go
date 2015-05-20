package machinery

import (
	"errors"
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

// Launch starts a new worker process
// The worker subscribes to the default queue
// and processes incoming registered tasks
func (worker *Worker) Launch() error {
	cnf := worker.server.GetConfig()

	log.Printf("Launching a worker with the following settings:")
	log.Printf("- Broker: %s", cnf.Broker)
	log.Printf("- ResultBackend: %s", cnf.ResultBackend)
	log.Printf("- Exchange: %s", cnf.Exchange)
	log.Printf("- ExchangeType: %s", cnf.ExchangeType)
	log.Printf("- DefaultQueue: %s", cnf.DefaultQueue)
	log.Printf("- BindingKey: %s", cnf.BindingKey)

	return worker.server.GetBroker().StartConsuming(worker.ConsumerTag, worker)
}

// Quit ...
func (worker *Worker) Quit() {
	worker.server.GetBroker().StopConsuming()
}

// Process handles received tasks and triggers success/error callbacks
func (worker *Worker) Process(signature *signatures.TaskSignature) {
	task := worker.server.GetRegisteredTask(signature.Name)
	if task == nil {
		log.Printf("Task with a name '%s' not registered", signature.Name)
		return
	}

	// Update task state to RECEIVED
	receivedState := backends.NewReceivedTaskState(signature.UUID)
	if err := worker.server.UpdateTaskState(receivedState); err != nil {
		worker.finalizeError(signature, err)
		return
	}

	// Get task args and convert them to proper types
	reflectedTask := reflect.ValueOf(task)
	relfectedArgs, err := worker.reflectArgs(signature.Args)
	if err != nil {
		worker.finalizeError(signature, err)
		return
	}

	// Update task state to STARTED
	startedState := backends.NewStartedTaskState(signature.UUID)
	if err := worker.server.UpdateTaskState(startedState); err != nil {
		worker.finalizeError(signature, err)
		return
	}

	// Call the task passing in the correct arguments
	results := reflectedTask.Call(relfectedArgs)
	if !results[1].IsNil() {
		worker.finalizeError(signature, errors.New(results[1].String()))
		return
	}

	worker.finalizeSuccess(signature, results[0])
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
func (worker *Worker) finalizeSuccess(signature *signatures.TaskSignature, result reflect.Value) {
	// Update task state to SUCCESS
	successState := backends.NewSuccessTaskState(
		signature.UUID,
		&backends.TaskResult{
			Type:  result.Type().String(),
			Value: result.Interface(),
		},
	)
	if err := worker.server.UpdateTaskState(successState); err != nil {
		log.Print(err)
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
}

// Task failed, update state and trigger error callbacks
func (worker *Worker) finalizeError(signature *signatures.TaskSignature, err error) {
	// Update task state to FAILURE
	failureState := backends.NewFailureTaskState(signature.UUID, err.Error())
	if err := worker.server.UpdateTaskState(failureState); err != nil {
		log.Printf("Failed updating status to FAILURE. Error = %v", err)
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
}
