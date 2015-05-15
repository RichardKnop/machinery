//
// Sending a task
// --------------
//
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

package main

import (
	"flag"
	"fmt"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/errors"
	"github.com/RichardKnop/machinery/v1/signatures"
)

// Define flagss
var (
	configPath    = flag.String("c", "config.yml", "Path to a configuration file")
	broker        = flag.String("b", "amqp://guest:guest@localhost:5672/", "Broker URL")
	resultBackend = flag.String("r", "amqp", "Result backend")
	exchange      = flag.String("e", "machinery_exchange", "Durable, non-auto-deleted AMQP exchange name")
	exchangeType  = flag.String("t", "direct", "Exchange type - direct|fanout|topic|x-custom")
	defaultQueue  = flag.String("q", "machinery_tasks", "Ephemeral AMQP queue name")
	bindingKey    = flag.String("k", "machinery_task", "AMQP binding key")

	cnf    config.Config
	server *machinery.Server
)

func init() {
	// Parse the flags
	flag.Parse()

	cnf = config.Config{
		Broker:        *broker,
		ResultBackend: *resultBackend,
		Exchange:      *exchange,
		ExchangeType:  *exchangeType,
		DefaultQueue:  *defaultQueue,
		BindingKey:    *bindingKey,
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
}

func main() {
	task1 := signatures.TaskSignature{
		Name: "add",
		Args: []signatures.TaskArg{
			signatures.TaskArg{
				Type:  "float64",
				Value: interface{}(1),
			},
			signatures.TaskArg{
				Type:  "float64",
				Value: interface{}(1),
			},
		},
	}

	task2 := signatures.TaskSignature{
		Name: "add",
		Args: []signatures.TaskArg{
			signatures.TaskArg{
				Type:  "float64",
				Value: interface{}(5),
			},
			signatures.TaskArg{
				Type:  "float64",
				Value: interface{}(6),
			},
		},
	}

	task3 := signatures.TaskSignature{
		Name: "multiply",
		Args: []signatures.TaskArg{
			signatures.TaskArg{
				Type:  "float64",
				Value: interface{}(4),
			},
		},
	}

	chain := machinery.Chain(task1, task2, task3)
	asyncResult, err := server.SendTask(chain)
	errors.Fail(err, "Could not send task")

	result, _ := asyncResult.Get()
	fmt.Printf("%v\n", result.Interface())
}
