[1]: https://raw.githubusercontent.com/RichardKnop/assets/master/machinery/example_worker.png
[2]: https://raw.githubusercontent.com/RichardKnop/assets/master/machinery/example_worker_receives_tasks.png

[![Codeship Status for RichardKnop/go-oauth2-server](https://codeship.com/projects/35dc5880-71a7-0133-ec05-06b1c29ec1d7/status?branch=master)](https://codeship.com/projects/116440)

[![GoDoc](https://img.shields.io/badge/godoc-reference-blue.svg)](http://godoc.org/github.com/RichardKnop/machinery/v1)
[![Travis Status for RichardKnop/machinery](https://travis-ci.org/RichardKnop/machinery.svg?branch=master)](https://travis-ci.org/RichardKnop/machinery)

# Machinery

Machinery is an asynchronous task queue/job queue based on distributed message passing.

So called tasks (or jobs if you like) are executed concurrently either by many workers on many servers or multiple worker processes on a single server using Golang's goroutines.

* [First Steps](#first-steps)
* [Configuration](#configuration)
* [Server](#server)
* [Workers](#workers)
* [Tasks](#tasks)
  * [Registering Tasks](#registering-tasks)
  * [Signatures](#signatures)
  * [Supported Types](#supported-types)
  * [Sending Tasks](#sending-tasks)
  * [Get Pending Tasks](#get-pending-tasks)
  * [Keeping Results](#keeping-results)
* [Workflows](#workflows)
  * [Groups](#groups)
  * [Chords](#chords)
  * [Chains](#chains)
* [Development](#development)
  * [Requirements](#requirements)
  * [Dependencies](#dependencies)
  * [Testing](#testing)

## First Steps

Add the Machinery library to your $GOPATH/src:

```
go get github.com/RichardKnop/machinery
```

First, you will need to define some tasks. Look at sample tasks in `examples/tasks/tasks.go` to see a few examples.

Second, you will need to launch a worker process:

```
go run examples/worker/worker.go
```

![Example worker][1]

Finally, once you have a worker running and waiting for tasks to consume, send some tasks:

```
go run examples/send/send.go
```

You will be able to see the tasks being processed asynchronously by the worker:

![Example worker receives tasks][2]

## Configuration

Machinery has several configuration options. Configuration is encapsulated by a `Config` struct and injected as a dependency to objects that need it.

```go
// QueueBindingArguments arguments which are used when binding to the exchange
type QueueBindingArguments map[string]interface{}

// Config holds all configuration for our program
type Config struct {
    Broker                string                `yaml:"broker"`
	ResultBackend         string                `yaml:"result_backend"`
	ResultsExpireIn       int                   `yaml:"results_expire_in"`
	Exchange              string                `yaml:"exchange"`
	ExchangeType          string                `yaml:"exchange_type"`
	DefaultQueue          string                `yaml:"default_queue"`
	QueueBindingArguments QueueBindingArguments `yaml:"queue_binding_arguments"`
	BindingKey            string                `yaml:"binding_key"`
	TLSConfig             *tls.Config
}

```

### Broker

A message broker. Currently supported brokers are:

#### AMQP 

Use AMQP URL in the format: 

````
amqp://[username:password@]@host[:port]
``` 

For example:

1. `amqp://guest:guest@localhost:5672`

#### Redis 

Use Redis URL in one of these formats: 

```
redis://[password@]host[port][/db_num]
redis+socket://[password@]/path/to/file.sock[:/db_num]
``` 

For example:

1. `redis://127.0.0.1:6379`, or with password `redis://password@127.0.0.1:6379`
2. `redis+socket://password@/path/to/file.sock:/0`

### ResultBackend

Result backend to use for keeping task states and results.

Currently supported backends are:

#### Redis 

Use Redis URL in one of these formats: 

```
redis://[password@]host[port][/db_num]
redis+socket://[password@]/path/to/file.sock[:/db_num]
``` 

For example:

1. `redis://127.0.0.1:6379`, or with password `redis://password@127.0.0.1:6379`
2. `redis+socket://password@/path/to/file.sock:/0`

#### Memcache 

Use Memcache URL in the format:

```
memcache://host1[:port1][,host2[:port2],...[,hostN[:portN]]]
``` 

For example:

1. `memcache://127.0.0.1:11211` for a single instance, or 
2. `memcache://10.0.0.1:11211,10.0.0.2:11211` for a cluster

#### AMQP 

Use AMQP URL in the format: 

````
amqp://[username:password@]@host[:port]
``` 

For example:

1. `amqp://guest:guest@localhost:5672`

> Keep in mind AMQP is not recommended as a result backend. See [Keeping Results](https://github.com/RichardKnop/machinery#keeping-results)

#### MongoDB 

Use Mongodb URL in the format: 

```
mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
``` 

For example:

1. `mongodb://127.0.0.1:27017/taskresults`

See [MongoDB docs](https://docs.mongodb.org/manual/reference/connection-string/) for more information.

### ResultsExpireIn

How long to store task results for in seconds. Defaults to `3600` (1 hour).

### Exchange

Exchange name, e.g. `machinery_exchange`. Only required for AMQP.

### ExchangeType

Exchange type, e.g. `direct`. Only required for AMQP.

### DefaultQueue

Default queue name, e.g. `machinery_tasks`.

### QueueBindingArguments

An optional map of additional arguments used when binding to an AMQP queue.

### BindingKey

The queue is bind to the exchange with this key, e.g. `machinery_task`. Only required for AMQP.

## Server

A Machinery library must be instantiated before use. The way this is done is by creating a `Server` instance. `Server` is a base object which stores Machinery configuration and registered tasks. E.g.:

```go
import (
    "github.com/RichardKnop/machinery/v1/config"
    machinery "github.com/RichardKnop/machinery/v1"
)

var cnf = config.Config{
    Broker:        "amqp://guest:guest@localhost:5672/",
    ResultBackend: "amqp://guest:guest@localhost:5672/",
    Exchange:      "machinery_exchange",
    ExchangeType:  "direct",
    DefaultQueue:  "machinery_tasks",
    BindingKey:    "machinery_task",
}

server, err := machinery.NewServer(&cnf)
if err != nil {
    // do something with the error
}
```

## Workers

In order to consume tasks, you need to have one or more workers running. All you need to run a worker is a `Server` instance with registered tasks. E.g.:

```go
worker := server.NewWorker("worker_name")
err := worker.Launch()
if err != nil {
    // do something with the error
}
```

Each worker will only consume registered tasks.

## Tasks

Tasks are a building block of Machinery applications. A task is a function which defines what happens when a worker receives a message. 

Currently each task needs to return two values, second one being `error`.

Let's say we want to define tasks for adding and multiplying numbers:

```go
func Add(args ...int64) (int64, error) {
    sum := int64(0)
    for _, arg := range args {
        sum += arg
    }
    return sum, nil
}

func Multiply(args ...int64) (int64, error) {
    sum := int64(1)
    for _, arg := range args {
        sum *= arg
    }
    return sum, nil
}
```

### Registering Tasks

Before your workers can consume a task, you need to register it with the server. This is done by assigning a task a unique name:

```go
server.RegisterTasks(map[string]interface{}{
    "add":      Add,
    "multiply": Multiply,
})
```

Tasks can also be registered one by one:

```go
server.RegisterTask("add", Add)
server.RegisterTask("multiply", Multiply)
```

Simply put, when a worker receives a message like this:

```json
{
    "UUID": "48760a1a-8576-4536-973b-da09048c2ac5",
    "Name": "add",
    "RoutingKey": "",
    "GroupUUID": "",
    "GroupTaskCount": 0,
    "Args": [
        {
            "Type": "int64",
            "Value": 1,
        },
        {
            "Type": "int64",
            "Value": 1,
        }
    ],
    "Immutable": false,
    "OnSuccess": null,
    "OnError": null,
    "ChordCallback": null
}
```

It will call Add(1, 1). Each task should return an error as well so we can handle failures.

Ideally, tasks should be idempotent which means there will be no unintended consequences when a task is called multiple times with the same arguments.

### Signatures

A signature wraps calling arguments, execution options (such as immutability) and success/error callbacks of a task so it can be sent across the wire to workers. Task signatures implement a simple interface:

```go
// TaskArg represents a single argument passed to invocation fo a task
type TaskArg struct {
	Type  string
	Value interface{}
}

// TaskHeaders represents the headers which should be used to direct the task
type TaskHeaders map[string]interface{}

// TaskSignature represents a single task invocation
type TaskSignature struct {
	UUID           string
	Name           string
	RoutingKey     string
	GroupUUID      string
	GroupTaskCount int
	Args           []TaskArg
	Headers        TaskHeaders
	Immutable      bool
	OnSuccess      []*TaskSignature
	OnError        []*TaskSignature
	ChordCallback  *TaskSignature
}
```

`UUID` is a unique ID of a task. You can either set it yourself or it will be automatically generated.

`Name` is the unique task name by which it is registered against a Server instance.

`RoutingKey` is used for routing a task to correct queue. If you leave it empty, the default behaviour will be to set it to the default queue's binding key for direct exchange type and to the default queue name for other exchange types.

`GroupUUID`, GroupTaskCount are useful for creating groups of tasks.

`Args` is a list of arguments that will be passed to the task when it is executed by a worker.

`Headers` is a list of headers that will be used when publishing the task to AMQP queue.

`Immutable` is a flag which defines whether a result of the executed task can be modified or not. This is important with `OnSuccess` callbacks. Immutable task will not pass its result to its success callbacks while a mutable task will prepend its result to args sent to callback tasks. Long story short, set Immutable to false if you want to pass result of the first task in a chain to the second task.

`OnSuccess` defines tasks which will be called after the task has executed successfully. It is a slice of task signature structs.

`OnError` defines tasks which will be called after the task execution fails. The first argument passed to error callbacks will be the error returned from the failed task.

`ChordCallback` is used to create a callback to a group of tasks.

### Supported Types

Machinery encodes tasks to JSON before sending them to the broker. Task results are also stored in the backend as JSON encoded strings. Therefor only types with native JSON representation can be supported. Currently supported types are:

* `bool`
* `int`
* `int8`
* `int16`
* `int32`
* `int64`
* `uint`
* `uint8`
* `uint16`
* `uint32`
* `uint64`
* `float32`
* `float64`
* `string`

### Sending Tasks

Tasks can be called by passing an instance of `TaskSignature` to an `Server` instance. E.g:

```go
import (
    "github.com/RichardKnop/machinery/v1/signatures"
)

task := signatures.TaskSignature{
    Name: "add",
    Args: []signatures.TaskArg{
        signatures.TaskArg{
            Type:  "int64",
            Value: 1,
        },
        signatures.TaskArg{
            Type:  "int64",
            Value: 1,
        },
    },
}

asyncResult, err := server.SendTask(&task1)
if err != nil {
    // failed to send the task
    // do something with the error
}

```

### Get Pending Tasks

Tasks currently waiting in the queue to be consumed by workers can be inspected, e.g.:

```go
server.GetBroker().GetPendingTasks("some_queue")
```

> Currently only supported by Redis broker.

### Keeping Results

If you configure a result backend, the task states and results will be persisted. Possible states:

```go
const (
    PendingState  = "PENDING"
    ReceivedState = "RECEIVED"
    StartedState  = "STARTED"
    SuccessState  = "SUCCESS"
    FailureState  = "FAILURE"
)
```

> When using AMQP as a result backend, task states will be persisted in separate queues for each task. Although RabbitMQ can scale up to thousands of queues, it is strongly advised to use a better suited result backend (e.g. Memcache) when you are expecting to run a large number of parallel tasks.

```go
// TaskResult represents an actual return value of a processed task
type TaskResult struct {
	Type  string
	Value interface{}
}

// TaskState represents a state of a task
type TaskState struct {
	TaskUUID string
	State    string
	Result   *TaskResult
	Error    string
}

// GroupMeta stores useful metadata about tasks within the same group
// E.g. UUIDs of all tasks which are used in order to check if all tasks
// completed successfully or not and thus whether to trigger chord callback
type GroupMeta struct {
	GroupUUID string
	TaskUUIDs []string
}

```

`TaskResult` represents a return value of a processed task.

`TaskState` struct will be serialised and stored every time a task state changes.

`GroupMeta` stores useful metadata about tasks within the same group. E.g. UUIDs of all tasks which are used in order to check if all tasks completed successfully or not and thus whether to trigger chord callback.

`AsyncResult` object allows you to check for the state of a task:

```go
taskState := asyncResult.GetState()
fmt.Printf("Current state of %v task is:\n", taskState.TaskUUID)
fmt.Println(taskState.State)
```

There are couple of convenient me methods to inspect the task status:

```go
asyncResult.GetState().IsCompleted()
asyncResult.GetState().IsSuccess()
asyncResult.GetState().IsFailure()
```

You can also do a synchronous blocking call to wait for a task result:

```go
result, err := asyncResult.Get()
if err != nil {
    // getting result of a task failed
    // do something with the error
}
fmt.Println(result.Interface())
```

## Workflows

Running a single asynchronous task is fine but often you will want to design a workflow of tasks to be executed in an orchestrated way. There are couple of useful functions to help you design workflows.

### Groups

`Group` is a set of tasks which will be executed in parallel, independent of each other. E.g.:

```go
import (
    "github.com/RichardKnop/machinery/v1/signatures"
    machinery "github.com/RichardKnop/machinery/v1"
)

task1 := signatures.TaskSignature{
    Name: "add",
    Args: []signatures.TaskArg{
        signatures.TaskArg{
            Type:  "int64",
            Value: 1,
        },
        signatures.TaskArg{
            Type:  "int64",
            Value: 1,
        },
    },
}

task2 := signatures.TaskSignature{
    Name: "add",
    Args: []signatures.TaskArg{
        signatures.TaskArg{
            Type:  "int64",
            Value: 5,
        },
        signatures.TaskArg{
            Type:  "int64",
            Value: 5,
        },
    },
}

group := machinery.NewGroup(&task1, &task2)
asyncResults, err := server.SendGroup(group)
if err != nil {
    // failed to send the group
    // do something with the error
}
```

`SendGroup` returns a slice of `AsyncResult` objects. So you can do a blocking call and wait for the result of groups tasks:

```go
for _, asyncResult := range asyncResults {
    result, err := asyncResult.Get()
    if err != nil {
        // getting result of a task failed
        // do something with the error
    }
    fmt.Println(result.Interface())
}
```

### Chords

`Chord` allows you to define a callback to be executed after all tasks in a group finished processing, e.g.:

```go
import (
    "github.com/RichardKnop/machinery/v1/signatures"
    machinery "github.com/RichardKnop/machinery/v1"
)

task1 := signatures.TaskSignature{
    Name: "add",
    Args: []signatures.TaskArg{
        signatures.TaskArg{
            Type:  "int64",
            Value: 1,
        },
        signatures.TaskArg{
            Type:  "int64",
            Value: 1,
        },
    },
}

task2 := signatures.TaskSignature{
    Name: "add",
    Args: []signatures.TaskArg{
        signatures.TaskArg{
            Type:  "int64",
            Value: 5,
        },
        signatures.TaskArg{
            Type:  "int64",
            Value: 5,
        },
    },
}

task3 := signatures.TaskSignature{
    Name: "multiply",
}

group := machinery.NewGroup(&task1, &task2)
chord := machinery.NewChord(group, &task3)
chordAsyncResult, err := server.SendChord(chord)
if err != nil {
    // failed to send the chord
    // do something with the error
}
```

The above example executes task1 and task2 in parallel, aggregates their results and passes them to task3. Therefore what would end up happening is:

```
multiply(add(1, 1), add(5, 5))
```

More explicitly:

```
(1 + 1) * (5 + 5) = 2 * 10 = 20
```

`SendChord` returns `ChordAsyncResult` which follows AsyncResult's interface. So you can do a blocking call and wait for the result of the callback:

```go
result, err := chordAsyncResult.Get()
if err != nil {
    // getting result of a chord failed
    // do something with the error
}
fmt.Println(result.Interface())
```

### Chains

`Chain` is simply a set of tasks which will be executed one by one, each successful task triggering the next task in the chain. E.g.:

```go
import (
    "github.com/RichardKnop/machinery/v1/signatures"
    machinery "github.com/RichardKnop/machinery/v1"
)

task1 := signatures.TaskSignature{
    Name: "add",
    Args: []signatures.TaskArg{
        signatures.TaskArg{
            Type:  "int64",
            Value: 1,
        },
        signatures.TaskArg{
            Type:  "int64",
            Value: 1,
        },
    },
}

task2 := signatures.TaskSignature{
    Name: "add",
    Args: []signatures.TaskArg{
        signatures.TaskArg{
            Type:  "int64",
            Value: 5,
        },
        signatures.TaskArg{
            Type:  "int64",
            Value: 5,
        },
    },
}

task3 := signatures.TaskSignature{
    Name: "multiply",
    Args: []signatures.TaskArg{
        signatures.TaskArg{
            Type:  "int64",
            Value: 4,
        },
    },
}

chain := machinery.NewChain(&task1, &task2, &task3)
chainAsyncResult, err := server.SendChain(chain)
if err != nil {
    // failed to send the chain
    // do something with the error
}
```

The above example executes task1, then task2 and then task3, passing the result of each task to the next task in the chain. Therefore what would end up happening is:

```
multiply(add(add(1, 1), 5, 5), 4)
```

More explicitly:

```
((1 + 1) + (5 + 5)) * 4 = 12 * 4 = 48
```

`SendChain` returns `ChainAsyncResult` which follows AsyncResult's interface. So you can do a blocking call and wait for the result of the whole chain:

```go
result, err := chainAsyncResult.Get()
if err != nil {
    // getting result of a chain failed
    // do something with the error
}
fmt.Println(result.Interface())
```

## Development

### Requirements

* Go
* RabbitMQ
* Redis (optional)
* Memcached (optional)

On OS X systems, you can install requirements using [Homebrew](http://brew.sh/):

```
brew install go
brew install rabbitmq
brew install redis
brew install memcached
```

### Dependencies

According to [Go 1.5 Vendor experiment](https://docs.google.com/document/d/1Bz5-UB7g2uPBdOx-rw5t9MxJwkfpx90cqG9AFL0JAYo), all dependencies are stored in the vendor directory. This approach is called `vendoring` and is the best practice for Go projects to lock versions of dependencies in order to achieve reproducible builds.

To update dependencies during development:

```
make update-deps
```

To install dependencies:

```
make install-deps
```

### Testing

To run tests:

```
$ make test
```

In order to enable integration tests, you will need to export few environment variables:

```
export AMQP_URL=amqp://guest:guest@localhost:5672/
export REDIS_URL=127.0.0.1:6379
export MEMCACHE_URL=127.0.0.1:11211
export MONGODB_URL=mongodb://{host}:{port}/{database}
```

I recommend to run the integration tests when making changes to the code. Due to Machinery being composed of several parts (worker, client) which run independently of each other, integration tests are important to verify everything works as expected.
