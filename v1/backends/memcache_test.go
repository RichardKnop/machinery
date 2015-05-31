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
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	go func() {
		backend := NewMemcacheBackend(&cnf, []string{memcacheURL})

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

	backend := NewMemcacheBackend(&cnf, []string{memcacheURL})

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
