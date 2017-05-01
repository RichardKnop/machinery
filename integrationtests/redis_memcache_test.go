package integrationtests

import (
	"fmt"
	"os"
	"testing"
)

func TestRedisMemcache(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	memcacheURL := os.Getenv("MEMCACHE_URL")
	if redisURL == "" || memcacheURL == "" {
		return
	}

	// Redis broker, Redis result backend
	server := _setup(fmt.Sprintf("redis://%v", redisURL), fmt.Sprintf("memcache://%v", memcacheURL))
	worker := server.NewWorker("test_worker")
	go worker.Launch()
	_testSendTask(server, t)
	_testSendGroup(server, t)
	_testSendChord(server, t)
	_testSendChain(server, t)
	_testReturnJustError(server, t)
	_testReturnMultipleValues(server, t)
	worker.Quit()
}
