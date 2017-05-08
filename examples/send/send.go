package main

import (
	"flag"
	"fmt"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// Define flagss
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

	cnf                                             config.Config
	server                                          *machinery.Server
	task0, task1, task2, task3, task4, task5, task6 tasks.Signature
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
		if err = config.ParseYAMLConfig(&data, &cnf); err != nil {
			log.FATAL.Fatal(err, "Could not parse config file")
		}
	}

	server, err = machinery.NewServer(&cnf)
	if err != nil {
		log.FATAL.Fatal(err, "Could not initialize server")
	}
}

func initTasks() {
	task0 = tasks.Signature{
		Name: "add",
		Args: []tasks.Arg{
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

	task1 = tasks.Signature{
		Name: "add",
		Args: []tasks.Arg{
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

	task2 = tasks.Signature{
		Name: "add",
		Args: []tasks.Arg{
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

	task3 = tasks.Signature{
		Name: "multiply",
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: 4,
			},
		},
	}

	task4 = tasks.Signature{
		Name: "multiply",
	}

	task5 = tasks.Signature{
		Name: "panic_task",
	}
}

func main() {
	/*
	 * First, let's try sending a single task
	 */
	initTasks()
	fmt.Println("Single task:")

	asyncResult, err := server.SendTask(&task0)
	if err != nil {
		log.FATAL.Fatal(err, "Could not send task")
	}

	results, err := asyncResult.Get()
	if err != nil {
		log.FATAL.Fatal(err, "Getting task state failed with error")
	}
	fmt.Printf("1 + 1 = %v\n", results[0].Interface())

	/*
	 * Now let's explore ways of sending multiple tasks
	 */

	// Now let's try a parallel execution
	initTasks()
	fmt.Println("Group of tasks (parallel execution):")

	group := tasks.NewGroup(&task0, &task1, &task2)
	asyncResults, err := server.SendGroup(group)
	if err != nil {
		log.FATAL.Fatal(err, "Could not send group")
	}

	for _, asyncResult := range asyncResults {
		results, err = asyncResult.Get()
		if err != nil {
			log.FATAL.Fatal(err, "Getting task state failed with error")
		}
		fmt.Printf(
			"%v + %v = %v\n",
			asyncResult.Signature.Args[0].Value,
			asyncResult.Signature.Args[1].Value,
			results[0].Interface(),
		)
	}

	// Now let's try a group with a chord
	initTasks()
	fmt.Println("Group of tasks with a callback (chord):")

	group = tasks.NewGroup(&task0, &task1, &task2)
	chord := tasks.NewChord(group, &task4)
	chordAsyncResult, err := server.SendChord(chord)
	if err != nil {
		log.FATAL.Fatal(err, "Could not send chord")
	}

	results, err = chordAsyncResult.Get()
	if err != nil {
		log.FATAL.Fatal(err, "Getting task state failed with error")
	}
	fmt.Printf("(1 + 1) * (2 + 2) * (5 + 6) = %v\n", results[0].Interface())

	// Now let's try chaining task results
	initTasks()
	fmt.Println("Chain of tasks:")

	chain := tasks.NewChain(&task0, &task1, &task2, &task3)
	chainAsyncResult, err := server.SendChain(chain)
	if err != nil {
		log.FATAL.Fatal(err, "Could not send chain")
	}

	results, err = chainAsyncResult.Get()
	if err != nil {
		log.FATAL.Fatal(err, "Getting chain result failed with error")
	}
	fmt.Printf("(((1 + 1) + (2 + 2)) + (5 + 6)) * 4 = %v\n", results[0].Interface())

	// Let's try a task which throws panic to make sure stack trace is not lost
	initTasks()
	asyncResult, err = server.SendTask(&task5)
	if err != nil {
		log.FATAL.Fatal(err, "Could not send task")
	}
}
