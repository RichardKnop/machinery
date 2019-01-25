package integration_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
)

func TestAmqpMongodb(t *testing.T) {
	amqpURL := os.Getenv("AMQP_URL")
	mongodbURL := os.Getenv("MONGODB_URL")
	if amqpURL == "" {
		t.Skip("AMQP_URL is not defined")
	}
	if mongodbURL == "" {
		t.Skip("MONGODB_URL is not defined")
	}

	// AMQP broker, MongoDB result backend
	server := testSetup(&config.Config{
		Broker:          amqpURL,
		DefaultQueue:    "test_queue",
		ResultsExpireIn: 30,
		ResultBackend:   fmt.Sprintf("mongodb://%v", mongodbURL),
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
