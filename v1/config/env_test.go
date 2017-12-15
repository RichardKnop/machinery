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

	cnf, err := config.NewFromEnvironment(false)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "broker", cnf.Broker)
	assert.Equal(t, "default_queue", cnf.DefaultQueue)
	assert.Equal(t, "result_backend", cnf.ResultBackend)
	assert.Equal(t, 123456, cnf.ResultsExpireIn)
	assert.Equal(t, "exchange", cnf.AMQP.Exchange)
	assert.Equal(t, "exchange_type", cnf.AMQP.ExchangeType)
	assert.Equal(t, "binding_key", cnf.AMQP.BindingKey)
	assert.Equal(t, "any", cnf.AMQP.QueueBindingArgs["x-match"])
	assert.Equal(t, "png", cnf.AMQP.QueueBindingArgs["image-type"])
	assert.Equal(t, 123, cnf.AMQP.PrefetchCount)
}
