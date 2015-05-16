package config

import (
	"bytes"
	"fmt"
	"log"
	"testing"
)

var configYAMLData = `---
broker: amqp://guest:guest@localhost:5672/
result_backend: amqp
results_expire_in: 3600000
exchange: machinery_exchange
exchange_type: direct
default_queue: machinery_tasks
binding_key: machinery_task
`

func TestReadFromFile(t *testing.T) {
	data, err := ReadFromFile("testconfig.yml")

	if string(data) == configYAMLData && err == nil {
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

	if cfg.Broker != "amqp://guest:guest@localhost:5672/" {
		log.Printf("%v", cfg)
		t.Errorf(
			"cfg.Broker = %v, want amqp://guest:guest@localhost:5672/",
			cfg.Broker,
		)
	}

	if cfg.ResultBackend != "amqp" {
		log.Printf("%v", cfg)
		t.Errorf(
			"cfg.ResultBackend = %v, want amqp",
			cfg.ResultBackend,
		)
	}

	if cfg.ResultsExpireIn != 3600000 {
		log.Printf("%v", cfg)
		t.Errorf(
			"cfg.ResultsExpireIn = %v, want 3600000",
			cfg.ResultsExpireIn,
		)
	}

	if cfg.Exchange != "machinery_exchange" {
		log.Printf("%v", cfg)
		t.Errorf(
			"cfg.Exchange = %v, want machinery_exchange",
			cfg.Exchange,
		)
	}

	if cfg.ExchangeType != "direct" {
		log.Printf("%v", cfg)
		t.Errorf(
			"cfg.ExchangeType = %v, want direct",
			cfg.ExchangeType,
		)
	}

	if cfg.DefaultQueue != "machinery_tasks" {
		log.Printf("%v", cfg)
		t.Errorf(
			"cfg.DefaultQueue = %v, want machinery_tasks",
			cfg.DefaultQueue,
		)
	}

	if cfg.BindingKey != "machinery_task" {
		log.Printf("%v", cfg)
		t.Errorf(
			"cfg.BindingKey = %v, want machinery_task",
			cfg.BindingKey,
		)
	}
}
