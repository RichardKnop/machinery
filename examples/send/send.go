/*
 * Sending a task
 * --------------
 *
 * This example demonstrates a simple workflow consisting of multiple
 * chained tasks. For each task you can specify success and error callbacks.
 *
 * When a task is successful, its return value will be prepended to args
 * of all success callbacks (signatures in TaskSignature.OnSuccess) and they
 * will then be sent to the queue.
 *
 * When a task fails, error will be prepended to all args of all error
 * callbacks (signatures in TaskSignature.OnError) and they
 * will then sent to the queue.
 *
 * The workflow bellow will result in ((1 + 1) + (5 + 6)) * 4 = 13 * 4 = 52
 */

package main

import (
	"flag"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
)

// Define flagss
var (
	configPath   = flag.String("c", "config.yml", "Path to a configuration file")
	brokerURL    = flag.String("b", "amqp://guest:guest@localhost:5672/", "Broker URL")
	exchange     = flag.String("e", "machinery_exchange", "Durable, non-auto-deleted AMQP exchange name")
	exchangeType = flag.String("t", "direct", "Exchange type - direct|fanout|topic|x-custom")
	defaultQueue = flag.String("q", "machinery_tasks", "Ephemeral AMQP queue name")
	bindingKey   = flag.String("k", "machinery_task", "AMQP binding key")

	cnf config.Config
	app *machinery.App
)

func init() {
	// Parse the flags
	flag.Parse()

	cnf = config.Config{
		BrokerURL:    *brokerURL,
		Exchange:     *exchange,
		ExchangeType: *exchangeType,
		DefaultQueue: *defaultQueue,
		BindingKey:   *bindingKey,
	}

	// Parse the config
	// NOTE: If a config file is present, it has priority over flags
	data, err := config.ReadFromFile(*configPath)
	if err == nil {
		config.ParseYAMLConfig(&data, &cnf)
	}

	app = machinery.InitApp(&cnf)
}

func main() {
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
