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

	cnf := config.Config{
		ResultBackend: memcacheURL,
	}

	signature := &signatures.TaskSignature{
		UUID: "taskUUID",
	}

	go func() {
		backend := NewMemcacheBackend(&cnf, []string{memcacheURL})

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

	backend := NewMemcacheBackend(&cnf, []string{memcacheURL})

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
