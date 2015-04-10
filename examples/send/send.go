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
	name := "foo"
	args := []interface{}{1, 2}
	kwargs := map[string]interface{}{
		"foo": "hello world",
		"bar": 123.4,
	}
	app.SendTask(name, args, kwargs)
}
