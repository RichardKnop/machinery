package main

import (
	"fmt"
	"os"
	"time"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/urfave/cli"

	exampletasks "github.com/RichardKnop/machinery/example/tasks"
)

var (
	app        *cli.App
	configPath string
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
			Value:       "",
			Destination: &configPath,
			Usage:       "Path to a configuration file",
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

func loadConfig() *config.Config {
	if configPath != "" {
		return config.NewFromYaml(configPath, true, true)
	}

	return config.NewFromEnvironment(true, true)
}

func startServer() (server *machinery.Server, err error) {
	// Create server instance
	server, err = machinery.NewServer(loadConfig())
	if err != nil {
		return
	}

	// Register tasks
	tasks := map[string]interface{}{
		"add":        exampletasks.Add,
		"multiply":   exampletasks.Multiply,
		"panic_task": exampletasks.PanicTask,
	}

	err = server.RegisterTasks(tasks)
	return
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
		return err
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

	results, err := asyncResult.Get(time.Duration(time.Millisecond * 5))
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
		results, err = asyncResult.Get(time.Duration(time.Millisecond * 5))
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

	results, err = chordAsyncResult.Get(time.Duration(time.Millisecond * 5))
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

	results, err = chainAsyncResult.Get(time.Duration(time.Millisecond * 5))
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
