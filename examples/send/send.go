/*
 * Sending a task
 * --------------
 *
 * This simple example demonstrates sending tasks to a Machinery worker.
 */

package main

import (
	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
)

var cnf = config.Config{
	BrokerURL:    "amqp://guest:guest@localhost:5672/",
	DefaultQueue: "task_queue",
}

func main() {
	app := machinery.InitApp(&cnf)

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

	task1 := machinery.TaskSignature{
		Name: "add",
		Args: []interface{}{1, 1},
	}

	task2 := machinery.TaskSignature{
		Name: "add",
		Args: []interface{}{5, 6},
	}

	task3 := machinery.TaskSignature{
		Name: "multiply",
		Args: []interface{}{4},
	}

	// Let's go!
	app.SendTask(machinery.Chain(task1, task2, task3))
}
