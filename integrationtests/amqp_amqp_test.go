package integrationtests

import (
	"os"
	"testing"
)

func TestAmqpAmqp(t *testing.T) {
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		return
	}

	// AMQP broker, AMQP result backend
	server := _setup(amqpURL, amqpURL)
	worker := server.NewWorker("test_worker")
	go worker.Launch()
	_testSendTask(server, t)
	_testSendGroup(server, t)
	_testSendChord(server, t)
	_testSendChain(server, t)
	worker.Quit()
}
