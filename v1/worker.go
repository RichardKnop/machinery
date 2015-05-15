package machinery

import (
	"encoding/json"
	"errors"
	"log"
	"reflect"

	"github.com/RichardKnop/machinery/v1/backends"
	"github.com/RichardKnop/machinery/v1/utils"
	"github.com/streadway/amqp"
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

	return worker.server.GetBroker().Consume(
		worker.ConsumerTag,
		worker,
	)
}

// ProcessMessage handles received messages
// First, it unmarshals the message into a TaskSignature
// Then, it looks whether the task is registered against the server
// If it is registered, it invokes the task using reflection and
// triggers success/error callbacks
func (worker *Worker) ProcessMessage(d *amqp.Delivery) {
	signature := TaskSignature{}
	err := json.Unmarshal([]byte(d.Body), &signature)
	if err != nil {
		log.Printf("Failed to unmarshal task singnature: %v", d.Body)
		return
	}

	task := worker.server.GetRegisteredTask(signature.Name)
	if task == nil {
		log.Printf("Task with a name '%s' not registered", signature.Name)
		return
	}

	worker.server.UpdateTaskState(signature.UUID, backends.ReceivedState, nil)

	errorFinalizer := func(err error) {
		worker.finalize(
			&signature,
			reflect.ValueOf(nil),
			err,
		)
	}

	reflectedTask := reflect.ValueOf(task)
	relfectedArgs, err := worker.reflectArgs(signature.Args)
	if err != nil {
		errorFinalizer(err)
		return
	}

	worker.server.UpdateTaskState(signature.UUID, backends.StartedState, nil)

	results := reflectedTask.Call(relfectedArgs)
	if !results[1].IsNil() {
		errorFinalizer(errors.New(results[1].String()))
		return
	}

	// Trigger success or error tasks
	worker.finalize(&signature, results[0], err)
}

// Converts []TaskArg to []reflect.Value
func (worker *Worker) reflectArgs(args []TaskArg) ([]reflect.Value, error) {
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
	signature *TaskSignature, result reflect.Value, err error,
) {
	if err != nil {
		worker.server.UpdateTaskState(signature.UUID, backends.FailureState, nil)

		log.Printf("Failed processing %s. Error = %v", signature.UUID, err)

		for _, errorTask := range signature.OnError {
			// Pass error as a first argument to error callbacks
			args := append([]TaskArg{TaskArg{
				Type:  reflect.TypeOf(err).String(),
				Value: reflect.ValueOf(err).Interface(),
			}}, errorTask.Args...)
			errorTask.Args = args
			worker.server.SendTask(errorTask)
		}
		return
	}

	worker.server.UpdateTaskState(
		signature.UUID,
		backends.SuccessState,
		&backends.TaskResult{
			Type:  result.Type().String(),
			Value: result.Interface(),
		},
	)

	log.Printf("Processed %s. Result = %v", signature.UUID, result.Interface())

	for _, successTask := range signature.OnSuccess {
		if signature.Immutable == false {
			// Pass results of the task to success callbacks
			args := append([]TaskArg{TaskArg{
				Type:  result.Type().String(),
				Value: result.Interface(),
			}}, successTask.Args...)
			successTask.Args = args
		}
		worker.server.SendTask(successTask)
	}
}
