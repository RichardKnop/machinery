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
	configPath = flag.String("c", "config.yml", "Path to a configuration file")
	// broker     = flag.String("b", "amqp://guest:guest@localhost:5672/", "Broker URL")
	broker       = flag.String("b", "redis://127.0.0.1:6379", "Broker URL")
	defaultQueue = flag.String("q", "machinery_tasks", "Ephemeral AMQP/Redis queue name")

	// resultBackend = flag.String("r", "amqp://guest:guest@localhost:5672/", "Result backend")
	resultBackend = flag.String("r", "redis://127.0.0.1:6379", "Result backend")
	// resultBackend = flag.String("r", "memcache://127.0.0.1:11211", "Result backend")
	// resultBackend = flag.String("r", "mongodb://127.0.0.1:27017", "Result backend")

	exchange      = flag.String("e", "machinery_exchange", "Durable, non-auto-deleted AMQP exchange name")
	exchangeType  = flag.String("t", "direct", "Exchange type - direct|fanout|topic|x-custom")
	bindingKey    = flag.String("k", "machinery_task", "AMQP binding key")
	prefetchCount = flag.Int("p", 3, "AMQP prefetch count")

	cnf    config.Config
	server *machinery.Server
	worker *machinery.Worker
)

func init() {
	// Parse the flags
	flag.Parse()

	cnf = config.Config{
		Broker:        *broker,
		DefaultQueue:  *defaultQueue,
		ResultBackend: *resultBackend,
		AMQP: &config.AMQPConfig{
			Exchange:      *exchange,
			ExchangeType:  *exchangeType,
			BindingKey:    *bindingKey,
			PrefetchCount: *prefetchCount,
		},
	}

	// Parse the config
	// NOTE: If a config file is present, it has priority over flags
	data, err := config.ReadFromFile(*configPath)
	if err == nil {
		err = config.ParseYAMLConfig(&data, &cnf)
		errors.Fail(err, "Could not parse config file")
	}

	server, err = machinery.NewServer(&cnf)
	errors.Fail(err, "Could not initialize server")

	// Register tasks
	tasks := map[string]interface{}{
		"add":        exampletasks.Add,
		"multiply":   exampletasks.Multiply,
		"panic_task": exampletasks.PanicTask,
	}
	server.RegisterTasks(tasks)

	// The second argument is a consumer tag
	// Ideally, each worker should have a unique tag (worker1, worker2 etc)
	worker = server.NewWorker("machinery_worker")
}

func main() {
	err := worker.Launch()
	errors.Fail(err, "Could not launch worker")
}
