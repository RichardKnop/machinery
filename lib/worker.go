package lib

import (
	"bytes"
	"time"
	"log"
	"github.com/streadway/amqp"
)

type Worker struct {
	app *App
}

func InitWorker(app *App) *Worker {
	return &Worker{
		app: app,
	}
}

func (worker *Worker) Listen() {
	conn, err := amqp.Dial(worker.app.BrokerURL)
	FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		worker.app.DefaultTaskQueue, 	// name
		true,         					// durable
		false,        					// delete when unused
		false,        					// exclusive
		false,        					// no-wait
		nil,          					// arguments
	)
	FailOnError(err, "Failed to declare a queue")

	err = ch.Qos(
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