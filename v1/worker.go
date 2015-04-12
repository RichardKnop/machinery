package v1

import (
	"bytes"
	"encoding/json"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// Worker represents a single worker process
type Worker struct {
	app *App
}

// InitWorker - worker constructor
func InitWorker(app *App) *Worker {
	return &Worker{
		app: app,
	}
}

// Launch starts a new worker process
// The worker subscribes to the default queue
// and processes any incoming tasks registered against the app
func (w *Worker) Launch() {
	log.Printf("Launching a worker with the following settings:")
	log.Printf("- BrokerURL: %s", w.app.config.BrokerURL)
	log.Printf("- DefaultQueue: %s", w.app.config.DefaultQueue)

	defer w.app.connection.Close()

	err := w.app.connection.Channel.Qos(
		3,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	FailOnError(err, "Failed to set QoS")

	deliveries, err := w.app.connection.Channel.Consume(
		w.app.connection.Queue.Name, // queue
		"worker",                    // consumer
		false,                       // auto-ack
		false,                       // exclusive
		false,                       // no-local
		false,                       // no-wait
		nil,                         // args
	)
	FailOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range deliveries {
			log.Printf("Received new message: %s", d.Body)
			d.Ack(false)
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			time.Sleep(t * time.Second)
			w.processMessage(&d)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
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
	result, err := task.Run(s.Args, s.Kwargs)

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
