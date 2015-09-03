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

func TestGroupCompletedAMQP(t *testing.T) {
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		return
	}

	groupUUID := "testGroupUUID"
	groupTaskCount := 2
	task1 := &signatures.TaskSignature{
		UUID:           "testTaskUUID1",
		GroupUUID:      groupUUID,
		GroupTaskCount: groupTaskCount,
	}
	task2 := &signatures.TaskSignature{
		UUID:           "testTaskUUID2",
		GroupUUID:      groupUUID,
		GroupTaskCount: groupTaskCount,
	}

	backend := NewAMQPBackend(amqpConfig)

	// Cleanup before the test
	backend.PurgeState(task1.UUID)
	backend.PurgeState(task2.UUID)
	backend.PurgeGroupMeta(groupUUID)

	groupCompleted, err := backend.GroupCompleted(groupUUID, groupTaskCount)
	if groupCompleted {
		t.Error("groupCompleted = true, should be false")
	}
	if err != nil {
		t.Errorf("err = %v, should be nil", err)
	}

	backend.InitGroup(groupUUID, []string{task1.UUID, task2.UUID})

	groupCompleted, _ = backend.GroupCompleted(groupUUID, groupTaskCount)
	if groupCompleted {
		t.Error("groupCompleted = true, should be false")
	}

	backend.SetStatePending(task1)
	backend.SetStateStarted(task2)
	groupCompleted, _ = backend.GroupCompleted(groupUUID, groupTaskCount)
	if groupCompleted {
		t.Error("groupCompleted = true, should be false")
	}

	backend.SetStateSuccess(task1, &TaskResult{})
	backend.SetStateSuccess(task2, &TaskResult{})
	groupCompleted, _ = backend.GroupCompleted(groupUUID, groupTaskCount)
	if !groupCompleted {
		t.Error("groupCompleted = false, should be true")
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
	taskState, err := backend.GetState(signature.UUID)
	if err != nil {
		t.Error(err)
	}

	backend.PurgeState(taskState.TaskUUID)
	taskState, err = backend.GetState(signature.UUID)
	if taskState != nil {
		t.Errorf("taskState = %v, want nil", taskState)
	}
	if err == nil {
		t.Error("Should have gotten error back")
	}
}
