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
)

// Define flags
var configPath = flag.String("c", "config.yml",
	"Path to a configuration file")
var brokerURL = flag.String("b", "amqp://guest:guest@localhost:5672/",
	"Broker URL")
var defaultQueue = flag.String("q", "task_queue",
	"Default task queue")

var cnf config.Config

func init() {
	// Parse the flags
	flag.Parse()

	cnf = config.Config{
		BrokerURL:    *brokerURL,
		DefaultQueue: *defaultQueue,
	}

	// Parse the config
	// NOTE: If a config file is present, it has priority over flags
	data, err := config.ReadFromFile(*configPath)
	if err == nil {
		config.ParseYAMLConfig(&data, &cnf)
	}
}

func main() {
	// Init the app from config
	app := machinery.InitApp(&cnf)

	// Register tasks to be processed by this worker
	tasks := map[string]machinery.Task{
		"add":      exampletasks.Add{},
		"multiply": exampletasks.Multiply{},
	}
	app.RegisterTasks(tasks)

	// Launch the worker!
	worker := machinery.InitWorker(app)
	worker.Launch()
}
