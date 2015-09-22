package integrationtests

import (
	"fmt"
	"os"
	"testing"
)

func TestAmqpMemcache(t *testing.T) {
	amqpURL := os.Getenv("AMQP_URL")
	memcacheURL := os.Getenv("MEMCACHE_URL")

	if amqpURL != "" && memcacheURL != "" {
		// AMQP broker, Memcache result backend
		server := _setup(amqpURL, fmt.Sprintf("memcache://%v", memcacheURL))
		worker := server.NewWorker("test_worker")
		go worker.Launch()
		_testSendTask(server, t)
		_testSendGroup(server, t)
		_testSendChord(server, t)
		_testSendChain(server, t)
		worker.Quit()
	}
}
