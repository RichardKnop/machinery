package lib

import (
	"bytes"
	"encoding/json"
	"log"
	"time"
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
func (worker *Worker) Launch() {
	log.Printf("Launching a worker with the following settings:")
	log.Printf("- BrokerURL: %s", worker.app.Config.BrokerURL)
	log.Printf("- DefaultQueue: %s", worker.app.Config.DefaultQueue)

	conn, ch, q := Connect(worker.app)
	defer conn.Close()
	defer ch.Close()

	err := ch.Qos(
		3,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	FailOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	FailOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			d.Ack(false)
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			time.Sleep(t * time.Second)
			worker.handleMessage(d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func (worker *Worker) handleMessage(body []byte) {
	message := make(map[string]interface{})
	json.Unmarshal([]byte(body), &message)

	if message["name"] == nil {
		log.Printf("Required field: name")
		return
	}
	if message["kwargs"] == nil {
		log.Printf("Required field: kwargs")
		return
	}

	name, ok := message["name"].(string)
	if !ok {
		log.Printf("Task name must be string")
		return
	}
	kwargs, ok := message["kwargs"].(map[string]interface{})
	if !ok {
		log.Printf("Kwargs must be [string]interface{}")
		return
	}

	task := worker.app.GetRegisteredTask(name)
	if task == nil {
		log.Printf("Task with a name '%s' not registered", message["name"])
		return
	}

	// Everything seems fine, process the task!
	task.Process(kwargs)
}
