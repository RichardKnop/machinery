package integration_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
)

func TestRedisSocket(t *testing.T) {
	redisSocket := os.Getenv("REDIS_SOCKET")
	if redisSocket == "" {
		t.Skip("REDIS_SOCKET is not defined")
	}

	// Redis broker, Redis result backend
	server := testSetup(&config.Config{
		Broker:        fmt.Sprintf("redis+socket://%v", redisSocket),
		DefaultQueue:  "test_queue",
		ResultBackend: fmt.Sprintf("redis+socket://%v", redisSocket),
	})
	worker := server.NewWorker("test_worker", 0)
	go worker.Launch()
	testAll(server, t)
	worker.Quit()
}
