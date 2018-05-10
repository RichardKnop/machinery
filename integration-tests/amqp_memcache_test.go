package integration_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
)

func TestAmqpMemcache(t *testing.T) {
	amqpURL := os.Getenv("AMQP_URL")
	memcacheURL := os.Getenv("MEMCACHE_URL")
	if amqpURL == "" {
		t.Skip("AMQP_URL is not defined")
	}
	if memcacheURL == "" {
		t.Skip("MEMCACHE_URL is not defined")
	}

	// AMQP broker, Memcache result backend
	server := testSetup(&config.Config{
		Broker:        amqpURL,
		DefaultQueue:  "test_queue",
		ResultBackend: fmt.Sprintf("memcache://%v", memcacheURL),
		AMQP: &config.AMQPConfig{
			Exchange:      "test_exchange",
			ExchangeType:  "direct",
			BindingKey:    "test_task",
			PrefetchCount: 1,
		},
	})
	worker := server.NewWorker("test_worker", 0)
	go worker.Launch()
	testAll(server, t)
	worker.Quit()
}
