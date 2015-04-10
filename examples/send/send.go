/*
 * Sending a task
 * --------------
 *
 * This simple example demonstrates sending a task to a Machinery worker.
 * In this case, a foobar task with foo and bar keyword arguments is sent.
 * It will be picked up and processed by one of worker processes.
 */

package main

import (
	"github.com/RichardKnop/machinery/v1"
)

var config = v1.Config{
	BrokerURL:    "amqp://guest:guest@localhost:5672/",
	DefaultQueue: "task_queue",
}

func main() {
	app := v1.InitApp(&config)

	// Send a test task
	workflow := v1.TaskSignature{
		Name: "add_task",
		Args: []interface{}{1, 2},
		Subsequent: []v1.TaskSignature{
			v1.TaskSignature{
				Name: "add_task",
				Args: []interface{}{5, 6},
			},
		},
	}
	app.SendTask(&workflow)
}
