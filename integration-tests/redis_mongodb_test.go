package integrationtests

import (
	"fmt"
	"os"
	"testing"
)

func TestRedisMongodb(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	mongodbURL := os.Getenv("MONGODB_URL")
	if redisURL == "" || mongodbURL == "" {
		return
	}

	// Redis broker, MongoDB result backend
	server := _setup(fmt.Sprintf("redis://%v", redisURL), fmt.Sprintf("mongodb://%v", mongodbURL))
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
