package config_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/stretchr/testify/assert"
)

var configYAMLData = `---
broker: amqp://guest:guest@localhost:5672/
result_backend: amqp
results_expire_in: 3600000
exchange: machinery_exchange
exchange_type: direct
default_queue: machinery_tasks
queue_binding_arguments:
  image-type: png
  x-match: any
binding_key: machinery_task
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
	assert.Equal(t, "amqp", cfg.ResultBackend)
	assert.Equal(t, 3600000, cfg.ResultsExpireIn)
	assert.Equal(t, "machinery_exchange", cfg.Exchange)
	assert.Equal(t, "direct", cfg.ExchangeType)
	assert.Equal(t, "machinery_tasks", cfg.DefaultQueue)
	assert.Equal(t, "machinery_task", cfg.BindingKey)
	assert.Equal(t, "any", cfg.QueueBindingArguments["x-match"])
	assert.Equal(t, "png", cfg.QueueBindingArguments["image-type"])
}
