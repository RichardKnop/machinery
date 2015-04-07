package main

import (
	"github.com/RichardKnop/machinery/lib"
)

func main() {
	app := lib.InitApp(
		"amqp://guest:guest@localhost:5672/",
		"task_queue",
	)

	app.SendTask("foobar")
}