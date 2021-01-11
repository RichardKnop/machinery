package integration_test

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"

	redisbackend "github.com/RichardKnop/machinery/v1/backends/redis"
	redisbroker "github.com/RichardKnop/machinery/v1/brokers/redis"
	eagerlock "github.com/RichardKnop/machinery/v1/locks/eager"
	machineryV2 "github.com/RichardKnop/machinery/v2"
)

func TestRedisRedis_Redigo(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	// Redis broker, Redis result backend
	server := testSetup(&config.Config{
		Broker:        fmt.Sprintf("redis://%v", redisURL),
		DefaultQueue:  "test_queue",
		ResultBackend: fmt.Sprintf("redis://%v", redisURL),
		Lock:          fmt.Sprintf("redis://%v", redisURL),
	})

	worker := server.(*machinery.Server).NewWorker("test_worker", 0)
	defer worker.Quit()
	go worker.Launch()
	testAll(server, t)
}

func TestRedisRedis_V2_GoRedis(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	cnf := &config.Config{
		DefaultQueue:    "machinery_tasks",
		ResultsExpireIn: 3600,
		Redis: &config.RedisConfig{
			MaxIdle:                3,
			IdleTimeout:            240,
			ReadTimeout:            15,
			WriteTimeout:           15,
			ConnectTimeout:         15,
			NormalTasksPollPeriod:  1000,
			DelayedTasksPollPeriod: 500,
		},
	}

	broker := redisbroker.NewGR(cnf, []string{redisURL}, 0)
	backend := redisbackend.NewGR(cnf, []string{redisURL}, 0)
	lock := eagerlock.New()
	server := machineryV2.NewServer(cnf, broker, backend, lock)

	registerTestTasks(server)

	worker := server.NewWorker("test_worker", 0)
	defer worker.Quit()
	go worker.Launch()
	testAll(server, t)
}

func TestRedisRedisNormalTaskPollPeriodLessThan1SecondShouldNotFailNextTask(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	// Redis broker, Redis result backend
	server := testSetup(&config.Config{
		Broker:        fmt.Sprintf("redis://%v", redisURL),
		DefaultQueue:  "test_queue",
		ResultBackend: fmt.Sprintf("redis://%v", redisURL),
		Lock:          fmt.Sprintf("redis://%v", redisURL),
		Redis: &config.RedisConfig{
			NormalTasksPollPeriod: 10, // 10 milliseconds
		},
	})

	worker := server.(*machinery.Server).NewWorker("test_worker", 0)
	go worker.Launch()
	defer worker.Quit()
	testSendTask(server, t)
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
			Lock:          fmt.Sprintf("redis://%v", redisURL),
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
		Lock:          fmt.Sprintf("redis://%v", redisURL),
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
	delta := time.Since(before)

	threshold := time.Duration(pollPeriod)*time.Millisecond + 1000 // add 1 second as buffer

	if delta.Nanoseconds() > threshold.Nanoseconds() {
		t.Error("Worker quit() exceeded timeout")
	}
}

func TestRedisRedisWorkerPreConsumeHandler(t *testing.T) {
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
		Lock:          fmt.Sprintf("redis://%v", redisURL),
		Redis: &config.RedisConfig{
			NormalTasksPollPeriod: pollPeriod, // default: 1000
		},
	}

	server, _ := machinery.NewServer(cnf)
	worker := server.NewWorker("test_worker", 0)
	errorsChan := make(chan error)
	err := errors.New("PreConsumeHandler is invoked")
	worker.SetPreConsumeHandler(func(*machinery.Worker) bool {
		errorsChan <- err
		return true
	})

	worker.LaunchAsync(errorsChan)
	if err != <-errorsChan {
		t.Error("PreConsumeHandler was not invoked")
	}
}
