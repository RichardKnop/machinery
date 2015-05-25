package backends

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
)

func TestGetStateAMQP(t *testing.T) {
	brokerURL := os.Getenv("AMQP_URL")
	if brokerURL == "" {
		return
	}

	cnf := config.Config{
		Broker:        brokerURL,
		ResultBackend: "amqp",
		Exchange:      "test_exchange",
		ExchangeType:  "direct",
		DefaultQueue:  "test_queue",
		BindingKey:    "test_task",
	}

	signature := &signatures.TaskSignature{
		UUID: "taskUUID",
	}

	go func() {
		backend := NewAMQPBackend(&cnf)

		pendingState := NewPendingTaskState(signature)
		backend.UpdateState(pendingState)

		time.Sleep(2 * time.Millisecond)

		receivedState := NewReceivedTaskState(signature)
		backend.UpdateState(receivedState)

		time.Sleep(2 * time.Millisecond)

		startedState := NewStartedTaskState(signature)
		backend.UpdateState(startedState)

		time.Sleep(2 * time.Millisecond)

		result := TaskResult{
			Type:  "float64",
			Value: 2,
		}
		successState := NewSuccessTaskState(signature, &result)
		backend.UpdateState(successState)
	}()

	backend := NewAMQPBackend(&cnf)

	for {
		taskState, err := backend.GetState(signature.UUID)

		if err != nil {
			log.Print(err)
			continue
		}

		if taskState.IsCompleted() {
			break
		}
	}
}
