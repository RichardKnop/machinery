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

	return worker.server.GetBroker().Consume(worker.ConsumerTag, worker)
}

// Process handles received tasks and triggers success/error callbacks
func (worker *Worker) Process(signature *signatures.TaskSignature) {
	task := worker.server.GetRegisteredTask(signature.Name)
	if task == nil {
		log.Printf("Task with a name '%s' not registered", signature.Name)
		return
	}

	worker.server.UpdateTaskState(
		signature.UUID,
		backends.ReceivedState,
		nil,
		nil,
	)

	errorFinalizer := func(theErr error) {
		worker.finalize(
			signature,
			reflect.ValueOf(nil),
			theErr,
		)
	}

	reflectedTask := reflect.ValueOf(task)
	relfectedArgs, err := worker.reflectArgs(signature.Args)
	if err != nil {
		errorFinalizer(err)
		return
	}

	worker.server.UpdateTaskState(
		signature.UUID,
		backends.StartedState,
		nil,
		nil,
	)

	results := reflectedTask.Call(relfectedArgs)
	if !results[1].IsNil() {
		errorFinalizer(errors.New(results[1].String()))
		return
	}

	// Trigger success or error tasks
	worker.finalize(signature, results[0], err)
}

// Converts []TaskArg to []reflect.Value
func (worker *Worker) reflectArgs(
	args []signatures.TaskArg,
) ([]reflect.Value, error) {
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

// finalize - handles success and error callbacks
func (worker *Worker) finalize(
	signature *signatures.TaskSignature, result reflect.Value, errResult error,
) {
	if errResult != nil {
		// Update task state to FAILURE
		worker.server.UpdateTaskState(
			signature.UUID,
			backends.FailureState,
			nil,
			errResult,
		)

		log.Printf("Failed processing %s. Error = %v", signature.UUID, errResult)

		for _, errorTask := range signature.OnError {
			// Pass error as a first argument to error callbacks
			args := append([]signatures.TaskArg{signatures.TaskArg{
				Type:  reflect.TypeOf(errResult).String(),
				Value: reflect.ValueOf(errResult).Interface(),
			}}, errorTask.Args...)
			errorTask.Args = args
			worker.server.SendTask(errorTask)
		}
		return
	}

	// Update task state to SUCCESS
	worker.server.UpdateTaskState(
		signature.UUID,
		backends.SuccessState,
		&backends.TaskResult{
			Type:  result.Type().String(),
			Value: result.Interface(),
		},
		nil,
	)

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
