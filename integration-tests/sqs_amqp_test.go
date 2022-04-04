package integration_test

import (
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
)

func TestSQSAmqp(t *testing.T) {
	sqsURL := os.Getenv("SQS_URL")
	if sqsURL == "" {
		t.Skip("SQS_URL is not defined")
	}

	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		t.Skip("AMQP_URL is not defined")
	}

	// AMQP broker, AMQP result backend
	server := testSetup(&config.Config{
		Broker:        sqsURL,
		DefaultQueue:  "test_queue",
		ResultBackend: amqpURL,
		Lock:          "eager",
		AMQP: &config.AMQPConfig{
			Exchange:      "test_exchange",
			ExchangeType:  "direct",
			BindingKey:    "test_task",
			PrefetchCount: 1,
		},
	})

	worker := server.(*machinery.Server).NewWorker("test_worker", 0)
	defer worker.Quit()
	go worker.Launch()
	testAll(server, t)
}
