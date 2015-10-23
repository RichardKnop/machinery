package backends

import (
	"os"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
)

func TestGroupCompletedRedis(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisURL == "" {
		return
	}

	groupUUID := "testGroupUUID"
	task1 := &signatures.TaskSignature{
		UUID:      "testTaskUUID1",
		GroupUUID: groupUUID,
	}
	task2 := &signatures.TaskSignature{
		UUID:      "testTaskUUID2",
		GroupUUID: groupUUID,
	}

	backend := NewRedisBackend(&config.Config{}, redisURL, redisPassword)

	// Cleanup before the test
	backend.PurgeState(task1.UUID)
	backend.PurgeState(task2.UUID)
	backend.PurgeGroupMeta(groupUUID)

	groupCompleted, err := backend.GroupCompleted(groupUUID, 2)
	if groupCompleted {
		t.Error("groupCompleted = true, should be false")
	}
	if err == nil {
		t.Errorf("err should not be nil")
	}

	backend.InitGroup(groupUUID, []string{task1.UUID, task2.UUID})

	groupCompleted, _ = backend.GroupCompleted(groupUUID, 2)
	if groupCompleted {
		t.Error("groupCompleted = true, should be false")
	}

	backend.SetStatePending(task1)
	backend.SetStateStarted(task2)
	groupCompleted, _ = backend.GroupCompleted(groupUUID, 2)
	if groupCompleted {
		t.Error("groupCompleted = true, should be false")
	}

	backend.SetStateStarted(task1)
	backend.SetStateSuccess(task2, &TaskResult{})
	groupCompleted, _ = backend.GroupCompleted(groupUUID, 2)
	if groupCompleted {
		t.Error("groupCompleted = true, should be false")
	}

	backend.SetStateFailure(task1, "Some error")
	groupCompleted, _ = backend.GroupCompleted(groupUUID, 2)
	if !groupCompleted {
		t.Error("groupCompleted = false, should be true")
	}
}

func TestGetStateRedis(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisURL == "" {
		return
	}

	signature := &signatures.TaskSignature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend := NewRedisBackend(&config.Config{}, redisURL, redisPassword)

	go func() {
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
	redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisURL == "" {
		return
	}

	signature := &signatures.TaskSignature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend := NewRedisBackend(&config.Config{}, redisURL, redisPassword)

	backend.SetStatePending(signature)
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
