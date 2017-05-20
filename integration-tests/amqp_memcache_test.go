package integrationtests

import (
	"fmt"
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
)

func TestAmqpMemcache(t *testing.T) {
	amqpURL := os.Getenv("AMQP_URL")
	memcacheURL := os.Getenv("MEMCACHE_URL")
	if amqpURL == "" || memcacheURL == "" {
		return
	}

	// AMQP broker, Memcache result backend
	server := setup(&config.Config{
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
	worker := server.NewWorker("test_worker")
	go worker.Launch()
	testSendTask(server, t)
	testSendGroup(server, t)
	testSendChord(server, t)
	testSendChain(server, t)
	testReturnJustError(server, t)
	testReturnMultipleValues(server, t)
	testPanic(server, t)
	worker.Quit()
}
