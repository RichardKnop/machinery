package lib

import (
	"bytes"
	"time"
	"log"
)

type Worker struct {
	app *App
}

func InitWorker(app *App) *Worker {
	return &Worker{
		app: app,
	}
}

func (worker *Worker) Launch() {
	log.Printf("Launching a worker with the following settings:")
	log.Printf("- broker_url: %s", worker.app.BrokerURL)
	log.Printf("- default_queue: %s", worker.app.DefaultQueue)

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
			dot_count := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dot_count)
			time.Sleep(t * time.Second)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}