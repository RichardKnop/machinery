package integration_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
)

func TestRedisRedis(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	// Redis broker, Redis result backend
	server := testSetup(&config.Config{
		Broker:        fmt.Sprintf("redis://%v", redisURL),
		DefaultQueue:  "test_queue",
		ResultBackend: fmt.Sprintf("redis://%v", redisURL),
	})
	worker := server.NewWorker("test_worker", 0)
	go worker.Launch()
	testAll(server, t)
	worker.Quit()
}

func TestRedisRedisWorkerQuitRaceCondition(t *testing.T) {
	repeat := 3
	for i := 0; i < repeat; i++ {
		redisURL := os.Getenv("REDIS_URL")
		if redisURL == "" {
			t.Skip("REDIS_URL is not defined")
		}

		// Redis broker, Redis result backend
		cnf := &config.Config{
			Broker:        fmt.Sprintf("redis://%v", redisURL),
			DefaultQueue:  "test_queue",
			ResultBackend: fmt.Sprintf("redis://%v", redisURL),
		}

		server, _ := machinery.NewServer(cnf)
		worker := server.NewWorker("test_worker", 0)

		errorsChan := make(chan error, 1)

		// Check Quit() immediately after LaunchAsync() will shutdown gracefully
		// and not panic on close(b.stopChan)
		worker.LaunchAsync(errorsChan)
		worker.Quit()

		if err := <-errorsChan; err != nil {
			t.Errorf("Error shutting down machinery worker gracefully %+v", err)
			continue
		}
	}
}

func TestRedisRedisWorkerQuickQuit(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	// Redis broker, Redis result backend
	pollPeriod := 1
	cnf := &config.Config{
		Broker:        fmt.Sprintf("redis://%v", redisURL),
		DefaultQueue:  "test_queue",
		ResultBackend: fmt.Sprintf("redis://%v", redisURL),
		Redis: &config.RedisConfig{
			NormalTasksPollPeriod: pollPeriod, // default: 1000
		},
	}

	server, _ := machinery.NewServer(cnf)
	worker := server.NewWorker("test_worker", 0)

	errorsChan := make(chan error, 1)

	// Check Quit() immediately after LaunchAsync() will shutdown gracefully
	// and not panic
	worker.LaunchAsync(errorsChan)

	before := time.Now()
	worker.Quit()
	delta := time.Now().Sub(before)

	threshold := time.Duration(pollPeriod)*time.Millisecond + 1000 // add 1 second as buffer

	if delta.Nanoseconds() > threshold.Nanoseconds() {
		t.Error("Worker quit() exceeded timeout")
	}
}
