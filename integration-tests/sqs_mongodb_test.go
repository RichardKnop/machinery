package integration_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
)

func TestSQSMongodb(t *testing.T) {
	sqsURL := os.Getenv("SQS_URL")
	mongodbURL := os.Getenv("MONGODB_URL")
	if sqsURL == "" || mongodbURL == "" {
		t.Error("shhhiiiit", sqsURL)
		return
	}

	// AMQP broker, MongoDB result backend
	server := testSetup(&config.Config{
		Broker:        sqsURL,
		DefaultQueue:  "test_queue",
		ResultBackend: fmt.Sprintf("mongodb://%v", mongodbURL),
	})
	worker := server.NewWorker("test_worker", 0)
	go worker.Launch()
	testAll(server, t)
	worker.Quit()
}
