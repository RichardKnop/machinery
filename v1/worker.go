package machinery

import (
	"encoding/json"
	"errors"
	"log"
	"reflect"

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
	log.Printf("- BrokerURL: %s", cnf.BrokerURL)
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
	json.Unmarshal([]byte(d.Body), &signature)

	task := worker.server.GetRegisteredTask(signature.Name)
	if task == nil {
		log.Printf("Task with a name '%s' not registered", signature.Name)
		return
	}

	// Everything seems fine, process the task!
	log.Printf("Started processing %s", signature.Name)

	errorFinalizer := func(err error) {
		worker.finalize(
			&signature,
			reflect.ValueOf(nil),
			err,
		)
	}

	reflectedTask := reflect.ValueOf(task)
	relfectedArgs, err := ReflectArgs(signature.Args)
	if err != nil {
		errorFinalizer(err)
		return
	}

	results := reflectedTask.Call(relfectedArgs)
	if !results[1].IsNil() {
		errorFinalizer(errors.New(results[1].String()))
		return
	}

	// Trigger success or error tasks
	worker.finalize(&signature, results[0], err)
}

// finalize - handles success and error callbacks
func (worker *Worker) finalize(
	signature *TaskSignature, result reflect.Value, err error,
) {
	if err != nil {
		log.Printf("Failed processing %s", signature.Name)
		log.Printf("Error = %v", err)

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

	log.Printf("Finished processing %s", signature.Name)
	log.Printf("Result = %v", result.Interface())

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
