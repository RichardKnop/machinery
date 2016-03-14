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
	resultBackend = flag.String("r", "amqp://guest:guest@localhost:5672/", "Result backend")
	exchange      = flag.String("e", "machinery_exchange", "Durable, non-auto-deleted AMQP exchange name")
	exchangeType  = flag.String("t", "direct", "Exchange type - direct|fanout|topic|x-custom")
	defaultQueue  = flag.String("q", "machinery_tasks", "Ephemeral AMQP queue name")
	bindingKey    = flag.String("k", "machinery_task", "AMQP binding key")

	cnf                                             config.Config
	server                                          *machinery.Server
	task0, task1, task2, task3, task4, task5, task6 signatures.TaskSignature
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

func initTasks() {
	task0 = signatures.TaskSignature{
		Name: "add",
		Args: []signatures.TaskArg{
			{
				Type:  "int64",
				Value: 1,
			},
			{
				Type:  "int64",
				Value: 1,
			},
		},
	}

	task1 = signatures.TaskSignature{
		Name: "add",
		Args: []signatures.TaskArg{
			{
				Type:  "int64",
				Value: 2,
			},
			{
				Type:  "int64",
				Value: 2,
			},
		},
	}

	task2 = signatures.TaskSignature{
		Name: "add",
		Args: []signatures.TaskArg{
			{
				Type:  "int64",
				Value: 5,
			},
			{
				Type:  "int64",
				Value: 6,
			},
		},
	}

	task3 = signatures.TaskSignature{
		Name: "multiply",
		Args: []signatures.TaskArg{
			{
				Type:  "int64",
				Value: 4,
			},
		},
	}

	task4 = signatures.TaskSignature{
		Name: "multiply",
	}
}

func main() {
	/*
	 * First, let's try sending a single task
	 */
	initTasks()
	fmt.Println("Single task:")

	asyncResult, err := server.SendTask(&task0)
	errors.Fail(err, "Could not send task")

	result, err := asyncResult.Get()
	errors.Fail(err, "Getting task state failed with error")
	fmt.Printf("1 + 1 = %v\n", result.Interface())

	/*
	 * Now let's explore ways of sending multiple tasks
	 */

	// Now let's try a parallel execution
	initTasks()
	fmt.Println("Group of tasks (parallel execution):")

	group := machinery.NewGroup(&task0, &task1, &task2)
	asyncResults, err := server.SendGroup(group)
	errors.Fail(err, "Could not send group")

	for _, asyncResult := range asyncResults {
		result, err = asyncResult.Get()
		errors.Fail(err, "Getting task state failed with error")
		fmt.Printf(
			"%v + %v = %v\n",
			asyncResult.Signature.Args[0].Value,
			asyncResult.Signature.Args[1].Value,
			result.Interface(),
		)
	}

	// Now let's try a group with a chord
	initTasks()
	fmt.Println("Group of tasks with a callback (chord):")

	group = machinery.NewGroup(&task0, &task1, &task2)
	chord := machinery.NewChord(group, &task4)
	chordAsyncResult, err := server.SendChord(chord)
	errors.Fail(err, "Could not send chord")

	result, err = chordAsyncResult.Get()
	errors.Fail(err, "Getting task state failed with error")
	fmt.Printf("(1 + 1) * (2 + 2) * (5 + 6) = %v\n", result.Interface())

	// Now let's try chaining task results
	initTasks()
	fmt.Println("Chain of tasks:")

	chain := machinery.NewChain(&task0, &task1, &task2, &task3)
	chainAsyncResult, err := server.SendChain(chain)
	errors.Fail(err, "Could not send chain")

	result, err = chainAsyncResult.Get()
	errors.Fail(err, "Getting chain result failed with error")
	fmt.Printf("(((1 + 1) + (2 + 2)) + (5 + 6)) * 4 = %v\n", result.Interface())
}
