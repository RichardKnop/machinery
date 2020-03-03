package integration_test

import (
	"os"
	"time"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/RichardKnop/machinery/v1/backends/result"
)

func TestAmqpGetPendingTasks(t *testing.T) {
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
		AMQP: &config.AMQPConfig{
			Exchange:      "test_exchange",
			ExchangeType:  "direct",
			BindingKey:    "test_task",
			PrefetchCount: 1,
		},
	})

	var results []*result.AsyncResult
	signatures := []*tasks.Signature{newAddTask(1, 2), newAddTask(3, 5), newAddTask(6, 7)}
	for _, s := range signatures {
		ar, err := server.SendTask(s)
		if err != nil {
			t.Error(err)
		}
		results = append(results, ar)
	}
	pendingMessages, err := server.GetBroker().GetPendingTasks(server.GetConfig().DefaultQueue)
	if err != nil {
		t.Error(err)
	}

	if len(pendingMessages) != len(signatures) {
		t.Errorf(
			"%d pending messages, should be %d",
			len(pendingMessages),
			len(signatures),
		)
	}
	for i := 0; i < len(signatures); i++ {
		compareSigs(t, signatures[i], pendingMessages[i])
	}

	worker := server.NewWorker("test_worker", 0)
	go worker.Launch()
	for _, r := range results {
		r.Get(time.Duration(time.Millisecond * 5))
	}
	worker.Quit()

	pendingMessages, err = server.GetBroker().GetPendingTasks(server.GetConfig().DefaultQueue)
	if err != nil {
		t.Error(err)
	}

	if len(pendingMessages) != 0 {
		t.Errorf(
			"%d pending messages, should be 0",
			len(pendingMessages),
		)
	}
}

func compareSigs(t *testing.T, a *tasks.Signature, b *tasks.Signature) {
	if a.UUID != b.UUID {
		t.Errorf("UUID mismatch, %v != %v", a.UUID, b.UUID)
	}
	if a.Name != b.Name {
		t.Errorf("UUID mismatch, %v != %v", a.Name, b.Name)
	}
	if len(a.Args) != len(b.Args) {
		t.Errorf("Arg length mismatch, %v != %v", len(a.Args), len(b.Args))
	}
}
