package integration_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
)

func TestRedisMongodb(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	mongodbURL := os.Getenv("MONGODB_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}
	if mongodbURL == "" {
		t.Skip("MONGODB_URL is not defined")
	}

	// Redis broker, MongoDB result backend
	server := testSetup(&config.Config{
		Broker:          fmt.Sprintf("redis://%v", redisURL),
		DefaultQueue:    "test_queue",
		ResultsExpireIn: 30,
		ResultBackend:   fmt.Sprintf("mongodb://%v", mongodbURL),
		Lock:            fmt.Sprintf("redis://%v", redisURL),
	})

	worker := server.(*machinery.Server).NewWorker("test_worker", 0)
	defer worker.Quit()
	go worker.Launch()
	testAll(server, t)
}
