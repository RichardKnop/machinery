package integration_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
)

func TestRedisMemcache(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	memcacheURL := os.Getenv("MEMCACHE_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}
	if memcacheURL == "" {
		t.Skip("MEMCACHE_URL is not defined")
	}

	// Redis broker, Redis result backend
	server := testSetup(&config.Config{
		Broker:        fmt.Sprintf("redis://%v", redisURL),
		DefaultQueue:  "test_queue",
		ResultBackend: fmt.Sprintf("memcache://%v", memcacheURL),
	})
	worker := server.NewWorker("test_worker", 0)
	go worker.Launch()
	testAll(server, t)
	worker.Quit()
}
