package config_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/stretchr/testify/assert"
)

var configYAMLData = `---
broker: "amqp://guest:guest@localhost:5672/"
default_queue: machinery_tasks
max_worker_instances: 10
result_backend: amqp
results_expire_in: 3600000
amqp:
  binding_key: machinery_task
  exchange: machinery_exchange
  exchange_type: direct
  prefetch_count: 3
  queue_binding_arguments:
    image-type: png
    x-match: any
`

func TestReadFromFile(t *testing.T) {
	data, err := config.ReadFromFile("testconfig.yml")

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
	cfg := new(config.Config)
	config.ParseYAMLConfig(&data, cfg)

	assert.Equal(t, "amqp://guest:guest@localhost:5672/", cfg.Broker)
	assert.Equal(t, "machinery_tasks", cfg.DefaultQueue)
	assert.Equal(t, "amqp", cfg.ResultBackend)
	assert.Equal(t, 3600000, cfg.ResultsExpireIn)
	assert.Equal(t, 10, cfg.MaxWorkerInstances)
	assert.Equal(t, "machinery_exchange", cfg.AMQP.Exchange)
	assert.Equal(t, "direct", cfg.AMQP.ExchangeType)
	assert.Equal(t, "machinery_task", cfg.AMQP.BindingKey)
	assert.Equal(t, "any", cfg.AMQP.QueueBindingArguments["x-match"])
	assert.Equal(t, "png", cfg.AMQP.QueueBindingArguments["image-type"])
	assert.Equal(t, 3, cfg.AMQP.PrefetchCount)
}
