package backends

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
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

	taskUUID := "taskUUID"

	go func() {
		backend := NewAMQPBackend(&cnf)

		pendingState := NewPendingTaskState(taskUUID)
		backend.UpdateState(pendingState)

		time.Sleep(2 * time.Millisecond)

		receivedState := NewReceivedTaskState(taskUUID)
		backend.UpdateState(receivedState)

		time.Sleep(2 * time.Millisecond)

		startedState := NewStartedTaskState(taskUUID)
		backend.UpdateState(startedState)

		time.Sleep(2 * time.Millisecond)

		result := TaskResult{
			Type:  "float64",
			Value: 2,
		}
		successState := NewSuccessTaskState(taskUUID, &result)
		backend.UpdateState(successState)
	}()

	backend := NewAMQPBackend(&cnf)

	for {
		taskState, err := backend.GetState(taskUUID)

		if err != nil {
			log.Print(err)
			continue
		}

		if taskState.IsCompleted() {
			break
		}
	}
}
