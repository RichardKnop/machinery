package backends

import (
	"os"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
)

func TestGetStateMemcache(t *testing.T) {
	memcacheURL := os.Getenv("MEMCACHE_URL")
	if memcacheURL == "" {
		return
	}

	signature := &signatures.TaskSignature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	go func() {
		backend := NewMemcacheBackend(&config.Config{}, []string{memcacheURL})

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

	backend := NewMemcacheBackend(&config.Config{}, []string{memcacheURL})

	for {
		taskState, err := backend.GetState(signature)

		if err != nil {
			continue
		}

		if taskState.IsCompleted() {
			break
		}
	}
}

func TestPurgeStateMemcache(t *testing.T) {
	memcacheURL := os.Getenv("MEMCACHE_URL")
	if memcacheURL == "" {
		return
	}

	signature := &signatures.TaskSignature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend := NewMemcacheBackend(&config.Config{}, []string{memcacheURL})

	backend.SetStatePending(signature)
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
