package machinery

import (
	"encoding/json"
	"log"

	"github.com/streadway/amqp"
)

// Worker represents a single worker process
type Worker struct {
	app *App
}

// InitWorker - Worker constructor
func InitWorker(app *App) *Worker {
	return &Worker{app: app}
}

// Launch starts a new worker process
// The worker subscribes to the default queue
// and processes any incoming tasks registered against the app
func (w *Worker) Launch() {
	cnf := w.app.GetConfig()
	conn := w.app.GetConnection()

	log.Printf("Launching a worker with the following settings:")
	log.Printf("- BrokerURL: %s", cnf.BrokerURL)
	log.Printf("- DefaultQueue: %s", cnf.DefaultQueue)

	conn.WaitForMessages(w)
}

// processMessage - handles received messages
// First, it unmarshals the message into a TaskSignature
// Then, it looks whether the task is registered against the app
// If it is registered, it calls signarute's Run method and then calls finalize
func (w *Worker) processMessage(d *amqp.Delivery) {
	s := TaskSignature{}
	json.Unmarshal([]byte(d.Body), &s)

	task := w.app.GetRegisteredTask(s.Name)
	if task == nil {
		log.Printf("Task with a name '%s' not registered", s.Name)
		return
	}

	// Everything seems fine, process the task!
	log.Printf("Started processing %s", s.Name)
	result, err := task.Run(s.Args)

	// Trigger success or error tasks
	w.finalize(&s, result, err)
}

// finalize - handles success and error callbacks
func (w *Worker) finalize(s *TaskSignature, result interface{}, err error) {
	if err != nil {
		log.Printf("Failed processing %s", s.Name)
		log.Printf("Error = %v", err)

		for _, errorTask := range s.OnError {
			// Pass error as a first argument to error callbacks
			args := append([]interface{}{err}, errorTask.Args...)
			errorTask.Args = args
			w.app.SendTask(&errorTask)
		}
		return
	}

	log.Printf("Finished processing %s", s.Name)
	log.Printf("Result = %v", result)

	for _, successTask := range s.OnSuccess {
		if s.Immutable == false {
			// Pass results of the task to success callbacks
			args := append([]interface{}{result}, successTask.Args...)
			successTask.Args = args
		}
		w.app.SendTask(&successTask)
	}
}
