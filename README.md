[![GoDoc](https://img.shields.io/badge/godoc-reference-blue.svg "GoDoc")](http://godoc.org/github.com/RichardKnop/machinery/v1)
![Build Status](https://travis-ci.org/RichardKnop/machinery.svg?branch=master)

Machinery
=========

Machinery is an asynchronous task queue/job queue based on distributed message passing.

So called tasks (or jobs if you like) are executed concurrently either by many workers on many servers or multiple worker processes on a single server using Golang's coroutines.

This is an early stage project so far. Feel free to contribute.

- [First Steps](https://github.com/RichardKnop/machinery#first-steps)
- [Server](https://github.com/RichardKnop/machinery#server)
- [Workers](https://github.com/RichardKnop/machinery#workers)
- [Tasks](https://github.com/RichardKnop/machinery#tasks)
    - [Registering Tasks](https://github.com/RichardKnop/machinery#registering-tasks)
    - [Signatures](https://github.com/RichardKnop/machinery#signatures)
    - [Sending Tasks](https://github.com/RichardKnop/machinery#sending-tasks)
- [Workflows](https://github.com/RichardKnop/machinery#workflows)
    - [Chains](https://github.com/RichardKnop/machinery#chains)
- [Development Setup](https://github.com/RichardKnop/machinery#development-setup)

First Steps
===========

Add the Machinery library to your $GOPATH/src:

```
$ go get github.com/RichardKnop/machinery
```

First, you will need to define some tasks. Look at sample tasks in examples/tasks/tasks.go to see few examples.

Second, you will need to launch a worker process:

```
$ go run examples/worker/worker.go
```

![Example worker](https://github.com/RichardKnop/machinery/blob/master/assets/example_worker.png)

Finally, once you have a worker running and waiting for tasks to consume, send some tasks:

```
$ go run examples/send/send.go
```

You will be able to see the tasks being processed asynchronously by the worker:

![Example worker receives tasks](https://github.com/RichardKnop/machinery/blob/master/assets/example_worker_receives_tasks.png)

Server
======

A Machinery library must be instantiated before use. The way this is done is by creating a Server instance. Server is a base object which stores Machinery configuration and registered tasks. E.g.:

```go
var cnf = config.Config{
	BrokerURL:    "amqp://guest:guest@localhost:5672/",
	Exchange:     "machinery_exchange",
	ExchangeType: "direct",
	DefaultQueue: "machinery_tasks",
	BindingKey:   "machinery_task",
}

server, err := machinery.NewServer(&cnf)
if err != nil {
    // do something with the error
}
```

Workers
=======

In order to consume tasks, you need to have one or more workers running. All you need to run a worker is a Server instance with registered tasks. E.g.:

```go
worker := server.NewWorker("worker_name")
err := worker.Launch()
if err != nil {
    // do something with the error
}
```

Each worker will only consume registered tasks.

Tasks
=====

Tasks are a building block of Machinery applications. A task is a function which defines what happens when a worker receives a message. Let's say we want to define tasks for adding and multiplying numbers:

```go
func Add(args ...float64) (float64, error) {
	sum := 0.0
	for _, arg := range args {
		sum += arg
	}
	return sum, nil
}

func Multiply(args ...float64) (float64, error) {
	sum := 1.0
	for _, arg := range args {
		sum *= arg
	}
	return sum, nil
}
```

Registering Tasks
-----------------

Before your workers can consume a task, you need to register it with the server. This is done by assigning a task a unique name:

```go
server.RegisterTasks(map[string]interface{}{
    "add":      Add,
    "multiply": Multiply,
})
```

Task can also be registered one by one:

```go
server.RegisterTask("add", Add)
server.RegisterTask("multiply", Multiply)
```

Simply put, when a worker receives a message like this:

```json
{
    "UUID": "48760a1a-8576-4536-973b-da09048c2ac5",
    "Name": "add",
    "Args": [
        {
            "Type": "float64",
            "Value": 1,
        },
        {
            "Type": "float64",
            "Value": 1,
        }
    ],
    "Immutable": false,
    "OnSuccess": null,
    "OnError": null
}
```

It will call Add(1, 1). Each task should return an error as well so we can handle failures.

Ideally, tasks should be idempotent which means there will be no unintended consequences when a task is called multiple times with the same arguments.

Signatures
----------

A signature wraps calling arguments, execution options (such as immutability) and success/error callbacks of a task so it can be send across the wire to workers. Task signatures implement a simple interface:

```go
type TaskArg struct {
	Type  string
	Value interface{}
}

type TaskSignature struct {
	UUID, Name, RoutingKey string
	Args                   []TaskArg
	Immutable              bool
	OnSuccess              []*TaskSignature
	OnError                []*TaskSignature
}
```

UUID is a unique ID of a task. You can either set it yourself or it will be automatically generated.

Name is the unique task name by which it is registered against a Server instance.

RoutingKey is used for routing a task to correct queue. If you leave it empty, the default behaviour will be to set it to the default queue's binding key for direct exchange type and to the default queue name for other exchange types.

Args is a list of arguments that will be passed to the task when it is executed by a worker.

Immutable is a flag which defines whether a result of the executed task can be modified or not. This is important with OnSuccess callbacks. Immutable task will not pass its result to its success callbacks while a mutable task will prepend its result to args sent to callback tasks. Long story short, set Immutable to false if you want to pass result of the first task in a chain to the second task.

OnSuccess defines tasks which will be called after the task has executed successfully. It is a slice of task signature structs.

OnError defines tasks which will be called after the task execution fails. The first argument passed to error callbacks will be the error returned from the failed task.

Sending Tasks
-------------

Tasks can be called by passing an instance of TaskSignature to an App instance. E.g:

```go
task := machinery.TaskSignature{
    Name: "add",
    Args: []machinery.TaskArg{
        machinery.TaskArg{
            Type:  "float64",
            Value: interface{}(1),
        },
        machinery.TaskArg{
            Type:  "float64",
            Value: interface{}(1),
        },
    },
}

asyncResult, err := server.SendTask(&task1)
if err != nil {
    // failed to send the task
    // do something with the error
}
```

Workflows
=========

Running a single asynchronous task is fine but often you will want to design a workflow of tasks to be executed in an orchestrated way. There are couple of useful functions to help you design workflows.

Chains
------

Chain is simply a set of tasks which will be executed one by one, each successful task triggering the next task in the chain. E.g.:

```go
task1 := machinery.TaskSignature{
    Name: "add",
    Args: []machinery.TaskArg{
        machinery.TaskArg{
            Type:  "float64",
            Value: interface{}(1),
        },
        machinery.TaskArg{
            Type:  "float64",
            Value: interface{}(1),
        },
    },
}

task2 := machinery.TaskSignature{
    Name: "add",
    Args: []machinery.TaskArg{
        machinery.TaskArg{
            Type:  "float64",
            Value: interface{}(5),
        },
        machinery.TaskArg{
            Type:  "float64",
            Value: interface{}(6),
        },
    },
}

task3 := machinery.TaskSignature{
    Name: "multiply",
    Args: []machinery.TaskArg{
        machinery.TaskArg{
            Type:  "float64",
            Value: interface{}(4),
        },
    },
}

chain := machinery.Chain(task1, task2, task3)
err := server.SendTask(chain)
if err != nil {
    // failed to send the task
    // do something with the error
}
```

The above example execute task1, then task2 and then task3, passing result of each task to the next task in the chain. Therefor what would end up happening is:

```
((1 + 1) + (5 + 6)) * 4 = 13 * 4 = 52
```

Development Setup
=================

First, there are several requirements:

- RabbitMQ
- Go

On OS X systems, you can install them using Homebrew:

```
$ brew install rabbitmq
$ brew install go
```

Then get all Machinery dependencies.

```
$ make deps
```

Tests
-----

```
$ make test
```
