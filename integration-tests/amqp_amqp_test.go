package integration_test

import (
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"

	amqpbackend "github.com/RichardKnop/machinery/v1/backends/amqp"
	amqpbroker "github.com/RichardKnop/machinery/v1/brokers/amqp"
	eagerlock "github.com/RichardKnop/machinery/v1/locks/eager"
	machineryV2 "github.com/RichardKnop/machinery/v2"
)

func TestAmqpAmqp(t *testing.T) {
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		t.Skip("AMQP_URL is not defined")
	}

	finalAmqpURL := amqpURL
	var finalSeparator string

	amqpURLs := os.Getenv("AMQP_URLS")
	if amqpURLs != "" {
		separator := os.Getenv("AMQP_URLS_SEPARATOR")
		if separator == "" {
			return
		}
		finalSeparator = separator
		finalAmqpURL = amqpURLs
	}

	// AMQP broker, AMQP result backend
	server := testSetup(&config.Config{
		Broker:                  finalAmqpURL,
		MultipleBrokerSeparator: finalSeparator,
		DefaultQueue:            "test_queue",
		ResultBackend:           amqpURL,
		Lock:                    "eager",
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

func TestAmqpAmqp_V2(t *testing.T) {
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		t.Skip("AMQP_URL is not defined")
	}

	finalAmqpURL := amqpURL
	var finalSeparator string

	amqpURLs := os.Getenv("AMQP_URLS")
	if amqpURLs != "" {
		separator := os.Getenv("AMQP_URLS_SEPARATOR")
		if separator == "" {
			return
		}
		finalSeparator = separator
		finalAmqpURL = amqpURLs
	}

	cnf := &config.Config{
		Broker:                  finalAmqpURL,
		MultipleBrokerSeparator: finalSeparator,
		DefaultQueue:            "machinery_tasks",
		ResultBackend:           amqpURL,
		ResultsExpireIn:         3600,
		AMQP: &config.AMQPConfig{
			Exchange:      "test_exchange",
			ExchangeType:  "direct",
			BindingKey:    "test_task",
			PrefetchCount: 1,
		},
	}

	broker := amqpbroker.New(cnf)
	backend := amqpbackend.New(cnf)
	lock := eagerlock.New()
	server := machineryV2.NewServer(cnf, broker, backend, lock)

	registerTestTasks(server)

	worker := server.NewWorker("test_worker", 0)
	defer worker.Quit()
	go worker.Launch()
	testAll(server, t)
}
