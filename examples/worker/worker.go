/*
 * A worker example
 * ----------------
 *
 * This is how a Machinery worker could look.
 *
 * Preferred way to launch a new worker process is by using a configuration file
 * (see config.yml in this directory for an example):
 * ./worker -c /path/to/config.yml
 *
 *
 * Optionally, you could pass command line flags:
 * ./worker -b amqp://guest:guest@localhost:5672/ -q tast_queue
 *
 * Once the worker process is up and running, it subscribes to the defined queue
 * and waits for incoming tasks. When a new task is published, the worker will
 * process it if it has been registered with the app.
 */

package main

import (
	"flag"

	"github.com/RichardKnop/machinery/examples/tasks"
	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/errors"
)

// Define flags
var (
	configPath   = flag.String("c", "config.yml", "Path to a configuration file")
	brokerURL    = flag.String("b", "amqp://guest:guest@localhost:5672/", "Broker URL")
	exchange     = flag.String("e", "machinery_exchange", "Durable, non-auto-deleted AMQP exchange name")
	exchangeType = flag.String("t", "direct", "Exchange type - direct|fanout|topic|x-custom")
	defaultQueue = flag.String("q", "machinery_tasks", "Ephemeral AMQP queue name")
	bindingKey   = flag.String("k", "machinery_task", "AMQP binding key")

	cnf    config.Config
	app    *machinery.App
	worker *machinery.Worker
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
		err = config.ParseYAMLConfig(&data, &cnf)
		errors.Fail(err, "Could not parse config file")
	}

	app, err := machinery.InitApp(&cnf)
	errors.Fail(err, "Could not init App")

	// Register tasks
	tasks := map[string]machinery.Task{
		"add":      exampletasks.AddTask{},
		"multiply": exampletasks.MultiplyTask{},
	}
	app.RegisterTasks(tasks)

	// The second argument is a consumer tag
	// Ideally, each worker should have a unique tag (worker1, worker2 etc)
	worker = machinery.InitWorker(app, "machinery_worker")
}

func main() {
	err := worker.Launch()
	errors.Fail(err, "Could not launch Worker")
}
