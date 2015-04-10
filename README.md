Machinery
=========

Machinery is an asynchronous task queue/job queue based on distributed message passing. It is similar in nature to Celery which is an excellent Python framework, although Machinery has been designed from gounr up and with Golang's strengths in mind.

So called tasks (or jobs if you like) are executed concurrently either by many workers on many servers or multiple worker processed on a single server using Golang's coroutines.

This is a very early stage project so far. Feel free to contribute.

Getting Started With Machinery
==============================

Requirements
------------

First, there are several requirements:

- RabbitMQ
- Go

On OS X systems, you can install them using Homebrew:

```
$ brew install rabbitmq
$ brew install go
```

Running Tests
-------------

```
$ go test ./lib
```

Installation
------------

First, you will need to download Machinery library to your GOPATH/src directory:

```
$ go get github.com/RichardKnop/machinery
```

Configuration
--------------

In order to use Machinery, you will need to define a worker and register some tasks.

Look at examples in examples/tasks/handlers.go to see a example tasks.

Once you defined your tasks, you will need to launch a new worker process:

```
$ go run examples/worker.go
```

Finally, open a new tab in your terminal and and send some tasks:

```
go run examples/send.go
```

You should be able to see tasks being asynchronously processed by the worker process :)
