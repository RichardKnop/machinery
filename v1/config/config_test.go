package config

import (
	"bytes"
	"fmt"
	"log"
	"testing"

	"github.com/JSainsburyPLC/issa-api/config"
)

var configYAMLData = `---
broker_url: amqp://guest:guest@localhost:5672/
default_queue: task_queue
`

func TestReadFromFile(t *testing.T) {
	data := config.ReadFromFile("testconfig.yml")

	if string(data) == configYAMLData {
		return
	}

	var buffer bytes.Buffer
	buffer.WriteString(
		fmt.Sprintf("Expected value:\n%v\n", configYAMLData))
	buffer.WriteString(
		fmt.Sprintf("Actual value:\n%v\n", string(data)))
	t.Error(buffer.String())
}

func TestParseYAMLConfig(t *testing.T) {
	data := []byte(configYAMLData)
	cfg := Config{}
	ParseYAMLConfig(&data, &cfg)

	if cfg.BrokerURL != "amqp://guest:guest@localhost:5672/" {
		log.Printf("%v", cfg)
		t.Errorf(
			"cfg.BrokerURL = %v, want amqp://guest:guest@localhost:5672/",
			cfg.BrokerURL,
		)
	}

	if cfg.DefaultQueue != "task_queue" {
		log.Printf("%v", cfg)
		t.Errorf(
			"cfg.DefaultQueue = %v, want task_queue",
			cfg.DefaultQueue,
		)
	}
}
