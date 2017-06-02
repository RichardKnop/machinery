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
  queue_binding_args:
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

func TestNewFromYaml(t *testing.T) {
	config.Reset()

	cnf := config.NewFromYaml("testconfig.yml", true, false)

	assert.Equal(t, "amqp://guest:guest@localhost:5672/", cnf.Broker)
	assert.Equal(t, "machinery_tasks", cnf.DefaultQueue)
	assert.Equal(t, "amqp", cnf.ResultBackend)
	assert.Equal(t, 3600000, cnf.ResultsExpireIn)
	assert.Equal(t, 10, cnf.MaxWorkerInstances)
	assert.Equal(t, "machinery_exchange", cnf.AMQP.Exchange)
	assert.Equal(t, "direct", cnf.AMQP.ExchangeType)
	assert.Equal(t, "machinery_task", cnf.AMQP.BindingKey)
	assert.Equal(t, "any", cnf.AMQP.QueueBindingArgs["x-match"])
	assert.Equal(t, "png", cnf.AMQP.QueueBindingArgs["image-type"])
	assert.Equal(t, 3, cnf.AMQP.PrefetchCount)
}
