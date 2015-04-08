package main

import (
	"flag"
	"github.com/RichardKnop/machinery/lib"
)

// Define flags
var configPath = flag.String("c", "config.yml", 
	"Path to a configuration file")
var brokerURL = flag.String("b", "amqp://guest:guest@localhost:5672/", 
	"Broker URL")
var defaultQueue = flag.String("q", "task_queue", 
	"Default task queue")

func main() {
	// Parse the flags
	flag.Parse()

	// Parse the config
	// NOTE: If a config file is present, it has priority over flags
	configMap := make(map[interface{}]interface{})
	configMap["broker_url"] = *brokerURL
	configMap["default_queue"] = *defaultQueue
	lib.ParseConfig(&configMap, configPath)

	// Init app
	app := lib.InitApp(configMap)

	// Launch a worker
	worker := lib.InitWorker(app)
	worker.Launch()
}