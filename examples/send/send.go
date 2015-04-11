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

	// This example demonstrates a simple workflow consisting of multiple
	// chained tasks. For each task you can specify success and error callbacks.
	//
	// When a task is successful, its return value will be prepended to args
	// of all success callbacks (signatures in TaskSignature.OnSuccess) and they
	// will then be sent to the queue.
	//
	// When a task fails, error will be prepended to all args of all error
	// callbacks (signatures in TaskSignature.OnError) and they
	// will then sent to the queue.
	//
	// The workflow bellow will result in ((1 + 1) + (5 + 6)) * 4 = 13 * 4 = 52

	task3 := v1.TaskSignature{
		Name: "multiply",
		Args: []interface{}{4},
	}

	task2 := v1.TaskSignature{
		Name:      "add",
		Args:      []interface{}{5, 6},
		OnSuccess: []v1.TaskSignature{task3},
	}

	task1 := v1.TaskSignature{
		Name:      "add",
		Args:      []interface{}{1, 1},
		OnSuccess: []v1.TaskSignature{task2},
	}

	// Let's go!
	app.SendTask(&task1)
}
