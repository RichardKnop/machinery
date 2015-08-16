package backends

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
)

var (
	amqpConfig *config.Config
)

func init() {
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		return
	}

	amqpConfig = &config.Config{
		Broker:        amqpURL,
		ResultBackend: amqpURL,
		Exchange:      "test_exchange",
		ExchangeType:  "direct",
		DefaultQueue:  "test_queue",
		BindingKey:    "test_task",
	}
}

func TestGetStateAMQP(t *testing.T) {
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		return
	}

	signature := &signatures.TaskSignature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	go func() {
		backend := NewAMQPBackend(amqpConfig)

		backend.SetStatePending(signature)

		time.Sleep(2 * time.Millisecond)

		backend.SetStateReceived(signature)

		time.Sleep(2 * time.Millisecond)

		backend.SetStateStarted(signature)

		time.Sleep(2 * time.Millisecond)

		taskResult := TaskResult{
			Type:  "float64",
			Value: 2,
		}
		backend.SetStateSuccess(signature, &taskResult)
	}()

	backend := NewAMQPBackend(amqpConfig)

	for {
		taskState, err := backend.GetState(signature)

		if err != nil {
			log.Print(err)
			continue
		}

		if taskState.IsCompleted() {
			break
		}
	}
}

func TestPurgeStateAMQP(t *testing.T) {
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		return
	}

	signature := &signatures.TaskSignature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend := NewAMQPBackend(amqpConfig)

	backend.SetStatePending(signature)
	backend.SetStateReceived(signature)
	taskState, err := backend.GetState(signature)
	if err != nil {
		t.Error(err)
	}

	backend.PurgeState(taskState)
	taskState, err = backend.GetState(signature)
	if taskState != nil {
		t.Errorf("taskState = %v, want nil", taskState)
	}
	if err == nil {
		t.Error("Should have gotten error back")
	}
}
