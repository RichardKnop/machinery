![Build Status](https://travis-ci.org/RichardKnop/machinery.svg?branch=master)

Machinery
=========

Machinery is an asynchronous task queue/job queue based on distributed message passing. It is similar in nature to Celery which is an excellent Python framework, although Machinery has been designed from ground up and with Golang's strengths in mind.

So called tasks (or jobs if you like) are executed concurrently either by many workers on many servers or multiple worker processes on a single server using Golang's coroutines.

This is an early stage project so far. Feel free to contribute.

- [First Steps](https://github.com/RichardKnop/machinery#first-steps)
- [Development Setup](https://github.com/RichardKnop/machinery#development-setup)

First Steps
===========

First, you will need to add the Machinery library to your $GOPATH/src:

```
$ go get github.com/RichardKnop/machinery
```

First, you will need to define some tasks.

Look at sample tasks in examples/tasks/tasks.go to see few examples.

Second, you will need to launch a worker process:

```
$ go run examples/worker/worker.go
```

Finally, once you have a worker running and waiting for tasks to consume, send some tasks:

```
$ go run examples/send/send.go
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
