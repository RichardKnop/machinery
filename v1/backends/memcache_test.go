package backends

import (
	"os"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
)

func TestUpdateGetState(t *testing.T) {
	memcacheURL := os.Getenv("MEMCACHE_URL")
	if memcacheURL == "" {
		return
	}

	cnf := config.Config{
		ResultBackend: memcacheURL,
	}

	taskUUID := "taskUUID"

	go func() {
		backend := NewMemcacheBackend(&cnf, []string{memcacheURL})

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
			Value: float64(2),
		}
		successState := NewSuccessTaskState(taskUUID, &result)
		backend.UpdateState(successState)
	}()

	backend := NewMemcacheBackend(&cnf, []string{memcacheURL})

	for {
		taskState, err := backend.GetState(taskUUID)

		if err != nil {
			continue
		}

		if taskState.IsCompleted() {
			break
		}
	}
}
