package integrationtests

import (
	"fmt"
	"os"
	"testing"
)

func TestRedisSocket(t *testing.T) {
	redisSocket := os.Getenv("REDIS_SOCKET")

	if redisSocket != "" {
		// Redis broker, Redis result backend
		server := _setup(fmt.Sprintf("redis+socket://%v", redisSocket), fmt.Sprintf("redis+socket://%v", redisSocket))
		worker := server.NewWorker("test_worker")
		go worker.Launch()
		_testSendTask(server, t)
		_testSendGroup(server, t)
		_testSendChord(server, t)
		_testSendChain(server, t)
		worker.Quit()
	}
}
