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
func (worker *Worker) Launch() {
	log.Printf("Launching a worker with the following settings:")
	log.Printf("- BrokerURL: %s", worker.app.config.BrokerURL)
	log.Printf("- DefaultQueue: %s", worker.app.config.DefaultQueue)

	c := worker.app.NewConnection().Open()
	defer c.Conn.Close()
	defer c.Channel.Close()

	err := c.Channel.Qos(
		3,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	FailOnError(err, "Failed to set QoS")

	deliveries, err := c.Channel.Consume(
		c.Queue.Name, // queue
		"worker",     // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
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
			worker.handleMessage(&d)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func (worker *Worker) handleMessage(d *amqp.Delivery) {
	msg := TaskMessage{}
	json.Unmarshal([]byte(d.Body), &msg)

	task := worker.app.GetRegisteredTask(msg.Name)
	if task == nil {
		log.Printf("Task with a name '%s' not registered", msg.Name)
		return
	}

	// Everything seems fine, process the task!
	log.Printf("Started processing %s", msg.Name)
	result := task.Run(msg.Args, msg.Kwargs)
	log.Printf("Finished processing %s", msg.Name)
	log.Printf("Result = %v", result)
}
