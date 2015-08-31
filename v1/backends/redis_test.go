package backends

import (
	"os"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
)

func TestGetStateRedis(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		return
	}

	signature := &signatures.TaskSignature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	go func() {
		backend := NewRedisBackend(&config.Config{}, redisURL)

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

	backend := NewRedisBackend(&config.Config{}, redisURL)

	for {
		taskState, err := backend.GetState(signature.UUID)

		if err != nil {
			continue
		}

		if taskState.IsCompleted() {
			break
		}
	}
}

func TestPurgeStateRedis(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		return
	}

	signature := &signatures.TaskSignature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend := NewRedisBackend(&config.Config{}, redisURL)

	backend.SetStatePending(signature)
	taskState, err := backend.GetState(signature.UUID)
	if err != nil {
		t.Error(err)
	}

	backend.PurgeState(taskState)
	taskState, err = backend.GetState(signature.UUID)
	if taskState != nil {
		t.Errorf("taskState = %v, want nil", taskState)
	}
	if err == nil {
		t.Error("Should have gotten error back")
	}
}
