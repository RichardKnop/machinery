package integrationtests

import (
	"fmt"
	"os"
	"testing"
)

func TestRedisRedis(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		return
	}

	// Redis broker, Redis result backend
	server := _setup(fmt.Sprintf("redis://%v", redisURL), fmt.Sprintf("redis://%v", redisURL))
	worker := server.NewWorker("test_worker")
	go worker.Launch()
	_testSendTask(server, t)
	_testSendGroup(server, t)
	_testSendChord(server, t)
	_testSendChain(server, t)
	worker.Quit()
}
