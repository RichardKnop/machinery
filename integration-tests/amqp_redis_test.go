package integrationtests

import (
	"fmt"
	"os"
	"testing"
)

func TestAmqpRedis(t *testing.T) {
	amqpURL := os.Getenv("AMQP_URL")
	redisURL := os.Getenv("REDIS_URL")
	if amqpURL == "" || redisURL == "" {
		return
	}

	// AMQP broker, Redis result backend
	server := _setup(amqpURL, fmt.Sprintf("redis://%v", redisURL))
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
