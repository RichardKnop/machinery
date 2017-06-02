package config_test

import (
	"bufio"
	"os"
	"strings"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/stretchr/testify/assert"
)

func TestNewFromEnvironment(t *testing.T) {
	config.Reset()

	file, err := os.Open("test.env")
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(file)
	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), "=")
		if len(parts) != 2 {
			continue
		}
		os.Setenv(parts[0], parts[1])
	}

	cnf := config.NewFromEnvironment(true, false)

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
