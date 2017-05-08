package main

import (
	"fmt"
	"os"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/urfave/cli"

	exampletasks "github.com/RichardKnop/machinery/example/tasks"
)

var (
	app               *cli.App
	configPath        string
	broker            string
	defaultQueue      string
	resultBackend     string
	amqpExchange      string
	amqpExchangeType  string
	amqpBindingKey    string
	amqpPrefetchCount int64
)

func init() {
	// Initialise a CLI app
	app = cli.NewApp()
	app.Name = "machinery"
	app.Usage = "machinery worker and send example tasks with machinery send"
	app.Author = "Richard Knop"
	app.Email = "risoknop@gmail.com"
	app.Version = "0.0.0"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "c",
			Value:       "config.yml",
			Destination: &configPath,
			Usage:       "Path to a configuration file",
		},
		cli.StringFlag{
			Name:        "b",
			Value:       "amqp://guest:guest@localhost:5672/",
			Destination: &broker,
			Usage:       "Broker URL",
		},
		cli.StringFlag{
			Name:        "q",
			Value:       "machinery_tasks",
			Destination: &defaultQueue,
			Usage:       "Ephemeral AMQP/Redis queue name",
		},
		cli.StringFlag{
			Name:        "r",
			Value:       "amqp://guest:guest@localhost:5672/",
			Destination: &resultBackend,
			Usage:       "Result backend",
		},
		cli.StringFlag{
			Name:        "e",
			Value:       "machinery_exchange",
			Destination: &amqpExchange,
			Usage:       "Durable, non-auto-deleted AMQP exchange name",
		},
		cli.StringFlag{
			Name:        "t",
			Value:       "direct",
			Destination: &amqpExchangeType,
			Usage:       "Exchange type - direct|fanout|topic|x-custom",
		},
		cli.StringFlag{
			Name:        "k",
			Value:       "machinery_task",
			Destination: &amqpBindingKey,
			Usage:       "AMQP binding key",
		},
		cli.Int64Flag{
			Name:        "p",
			Value:       int64(3),
			Destination: &amqpPrefetchCount,
			Usage:       "AMQP prefetch count",
		},
	}
}

func main() {
	// Set the CLI app commands
	app.Commands = []cli.Command{
		{
			Name:  "worker",
			Usage: "launch machinery worker",
			Action: func(c *cli.Context) error {
				return worker()
			},
		},
		{
			Name:  "send",
			Usage: "send example tasks ",
			Action: func(c *cli.Context) error {
				return send()
			},
		},
	}

	// Run the CLI app
	if err := app.Run(os.Args); err != nil {
		log.FATAL.Print(err)
	}
}

func startServer() (*machinery.Server, error) {
	cnf := config.Config{
		Broker:        broker,
		DefaultQueue:  defaultQueue,
		ResultBackend: resultBackend,
		AMQP: &config.AMQPConfig{
			Exchange:      amqpExchange,
			ExchangeType:  amqpExchangeType,
			BindingKey:    amqpBindingKey,
			PrefetchCount: int(amqpPrefetchCount),
		},
	}

	// If present, the config file takes priority over cli flags
	data, err := config.ReadFromFile(configPath)
	if err != nil {
		log.WARNING.Printf("Could not load config from file: %s", err.Error())
	} else {
		if err = config.ParseYAMLConfig(&data, &cnf); err != nil {
			return nil, fmt.Errorf("Could not parse config file: %s", err.Error())
		}
	}

	// Create server instance
	server, err := machinery.NewServer(&cnf)
	if err != nil {
		return nil, fmt.Errorf("Could not initialize server: %s", err.Error())
	}

	// Register tasks
	tasks := map[string]interface{}{
		"add":        exampletasks.Add,
		"multiply":   exampletasks.Multiply,
		"panic_task": exampletasks.PanicTask,
	}
	server.RegisterTasks(tasks)

	return server, nil
}

func worker() error {
	server, err := startServer()
	if err != nil {
		return err
	}

	// The second argument is a consumer tag
	// Ideally, each worker should have a unique tag (worker1, worker2 etc)
	worker := server.NewWorker("machinery_worker")

	if err := worker.Launch(); err != nil {
		return fmt.Errorf("Could not launch worker: %s", err.Error())
	}

	return nil
}

func send() error {
	server, err := startServer()
	if err != nil {
		return err
	}

	var task0, task1, task2, task3, task4, task5 tasks.Signature

	var initTasks = func() {
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

	/*
	 * First, let's try sending a single task
	 */
	initTasks()
	log.INFO.Println("Single task:")

	asyncResult, err := server.SendTask(&task0)
	if err != nil {
		return fmt.Errorf("Could not send task: %s", err.Error())
	}

	results, err := asyncResult.Get()
	if err != nil {
		return fmt.Errorf("Getting task result failed with error: %s", err.Error())
	}
	log.INFO.Printf("1 + 1 = %v\n", results[0].Interface())

	/*
	 * Now let's explore ways of sending multiple tasks
	 */

	// Now let's try a parallel execution
	initTasks()
	log.INFO.Println("Group of tasks (parallel execution):")

	group := tasks.NewGroup(&task0, &task1, &task2)
	asyncResults, err := server.SendGroup(group)
	if err != nil {
		return fmt.Errorf("Could not send group: %s", err.Error())
	}

	for _, asyncResult := range asyncResults {
		results, err = asyncResult.Get()
		if err != nil {
			return fmt.Errorf("Getting task result failed with error: %s", err.Error())
		}
		log.INFO.Printf(
			"%v + %v = %v\n",
			asyncResult.Signature.Args[0].Value,
			asyncResult.Signature.Args[1].Value,
			results[0].Interface(),
		)
	}

	// Now let's try a group with a chord
	initTasks()
	log.INFO.Println("Group of tasks with a callback (chord):")

	group = tasks.NewGroup(&task0, &task1, &task2)
	chord := tasks.NewChord(group, &task4)
	chordAsyncResult, err := server.SendChord(chord)
	if err != nil {
		return fmt.Errorf("Could not send chord: %s", err.Error())
	}

	results, err = chordAsyncResult.Get()
	if err != nil {
		return fmt.Errorf("Getting chord result failed with error: %s", err.Error())
	}
	log.INFO.Printf("(1 + 1) * (2 + 2) * (5 + 6) = %v\n", results[0].Interface())

	// Now let's try chaining task results
	initTasks()
	log.INFO.Println("Chain of tasks:")

	chain := tasks.NewChain(&task0, &task1, &task2, &task3)
	chainAsyncResult, err := server.SendChain(chain)
	if err != nil {
		return fmt.Errorf("Could not send chain: %s", err.Error())
	}

	results, err = chainAsyncResult.Get()
	if err != nil {
		return fmt.Errorf("Getting chain result failed with error: %s", err.Error())
	}
	log.INFO.Printf("(((1 + 1) + (2 + 2)) + (5 + 6)) * 4 = %v\n", results[0].Interface())

	// Let's try a task which throws panic to make sure stack trace is not lost
	initTasks()
	asyncResult, err = server.SendTask(&task5)
	if err != nil {
		return fmt.Errorf("Could not send task: %s", err.Error())
	}

	return nil
}
