package integrationtests

import (
	"fmt"
	"os"
	"testing"
)

func TestAmqpMongodb(t *testing.T) {
	amqpURL := os.Getenv("AMQP_URL")
	mongodbURL := os.Getenv("MONGODB_URL2")
	if amqpURL == "" || mongodbURL == "" {
		return
	}

	// AMQP broker, MongoDB result backend
	server := _setup(amqpURL, fmt.Sprintf("mongodb://%v", mongodbURL))
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
