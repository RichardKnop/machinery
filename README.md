[![GoDoc](https://img.shields.io/badge/godoc-reference-blue.svg "GoDoc")](http://godoc.org/github.com/RichardKnop/machinery/v1)
![Build Status](https://travis-ci.org/RichardKnop/machinery.svg?branch=master)

Machinery
=========

Machinery is an asynchronous task queue/job queue based on distributed message passing. It is similar in nature to Celery which is an excellent Python framework, although Machinery has been designed from ground up and with Golang's strengths in mind.

So called tasks (or jobs if you like) are executed concurrently either by many workers on many servers or multiple worker processes on a single server using Golang's coroutines.

This is an early stage project so far. Feel free to contribute.

- [First Steps](https://github.com/RichardKnop/machinery#first-steps)
- [App](https://github.com/RichardKnop/machinery#app)
- [Workers](https://github.com/RichardKnop/machinery#workers)
- [Tasks](https://github.com/RichardKnop/machinery#tasks)
    - [Registering Tasks](https://github.com/RichardKnop/machinery#registering-tasks)
    - [Signatures](https://github.com/RichardKnop/machinery#signatures)
    - [Calling Tasks](https://github.com/RichardKnop/machinery#calling-tasks)
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

Finally, once you have a worker running and waiting for tasks to consume, send some tasks:

```
$ go run examples/send/send.go
```

App
===

A Machinery library must be instantiated before use. The way this is done is by creating an App instance. App is a base object which stores Machinery configuration and registered tasks. E.g.:

```go
var cnf = config.Config{
	BrokerURL:    "amqp://guest:guest@localhost:5672/",
	DefaultQueue: "task_queue",
}

app := machinery.InitApp(&cnf)
```

Workers
=======

In order to consume tasks, you need to have one or more workers running. All you need to run a worker is an App instance with registered tasks. E.g.:

```go
worker := machinery.InitWorker(app)
worker.Launch()
```

Each worker will only consume registered tasks.

Tasks
=====

Tasks are a building block of Machinery applications. A task is a struct which implements a simple interface:

```go
type Task interface {
	Run(args []interface{}) (interface{}, error)
}
```

A task defines what happens when a worker receives a message. Let's say we want to define a task that will add all numbers passed to it and return their sum:

```go
type AddTask struct{}

func (t AddTask) Run(args []interface{}) (interface{}, error) {
	parsedArgs, err := machinery.ParseNumberArgs(args)
	if err != nil {
		return nil, err
	}

	add := func(args []float64) float64 {
		sum := 0.0
		for _, arg := range args {
			sum += arg
		}
		return sum
	}

	return add(parsedArgs), nil
}

// ... more tasks
```

Registering Tasks
-----------------

Before your workers can consume a task, you need to register it with an App instance. This is done by assigning a task signature a unique name and registering with an App instance:

```go
tasks := map[string]machinery.Task{
    "add":      mytasks.AddTask{},
    "multiply": mytasks.MultiplyTask{},
}
app.RegisterTasks(tasks)
```

Task can also be registered one by one:

```go
app.RegisterTask("add", mytasks.AddTask{})
app.RegisterTask("multiply", mytasks.MultiplyTask{})
```

The above code snippet would register two tasks against the App instance:

* add: mytasks.AddTask
* multiply: mytasks.MultiplyTask

Simply put, when a worker receives a message like this:

```json
{
    "Name": "add",
    "Args": [1, 1],
    "Immutable": false,
    "OnSuccess": null,
    "OnError": null
}
```

It will call Run method of mytasks.AddTask with [1, 1] arguments.

Ideally, tasks should be idempotent which means there will be no unintended consequences when a task is called multiple times with the same arguments.

Signatures
----------

A signature wraps calling arguments, execution options (such as immutability) and success/error callbacks of a task so it can be send across the wire to workers. Task signatures implement a simple interface:

```go
type TaskSignature struct {
	Name      string
	Args      []interface{}
	Immutable bool
	OnSuccess []TaskSignature
	OnError   []TaskSignature
}
```

Name is the unique task name by which it is registered against a Machinery app.

Args is a list of arguments that will be passed to the task when it is executed by a worker.

Immutable is a flag which defines whether a result of the executed task can be modified or not. This is important with OnSuccess callbacks. Immutable task will not pass its result to its success callbacks while a mutable task will prepend its result to args sent to callback tasks. Long story short, set Immutable to false if you want to pass result of the first task in a chain to the second task.

OnSuccess defines tasks which will be called after the task has executed successfully. It is a slice of task signature structs.

OnError defines tasks which will be called after the task execution fails. The first argument passed to error callbacks will be the error returned from the failed task.

Sending Tasks
-------------

Tasks can be called by passing an instance of TaskSignature to an App instance. E.g:

```go
task := machinery.TaskSignature{
    Name:      "add",
    Args:      []interface{}{1, 1},
}

app.SendTask(&task1)
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
