package lib

import (
	"bytes"
	"fmt"
	"log"
	"testing"
)

var configYAMLData = `---
brokerurl: amqp://guest:guest@localhost:5672/
defaultqueue: task_queue
`

func TestReadFromFile(t *testing.T) {
	data := ReadFromFile("./_testconfig.yml")

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
	config := Config{}
	ParseYAMLConfig(&data, &config)

	if config.BrokerURL != "amqp://guest:guest@localhost:5672/" {
		log.Printf("%v", config)
		t.Errorf(
			"config.BrokerURL = %v, want amqp://guest:guest@localhost:5672/",
			config.BrokerURL,
		)
	}

	if config.DefaultQueue != "task_queue" {
		log.Printf("%v", config)
		t.Errorf(
			"config.DefaultQueue = %v, want task_queue",
			config.DefaultQueue,
		)
	}
}
