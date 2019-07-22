package integration_test

import (
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

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

func TestRedisRedis_NormalTasksPollPeriod(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	normalTasksPollPeriod := 5

	// Redis broker, Redis result backend
	var server = testSetup(&config.Config{
		Broker:        fmt.Sprintf("redis://%v", redisURL),
		DefaultQueue:  "test_queue",
		ResultBackend: fmt.Sprintf("redis://%v", redisURL),
		Redis: &config.RedisConfig{
			MaxIdle:                3,
			IdleTimeout:            240,
			ReadTimeout:            15,
			WriteTimeout:           15,
			ConnectTimeout:         15,
			DelayedTasksPollPeriod: 20,
			NormalTasksPollPeriod:  normalTasksPollPeriod, // Affected by ReadTimeout
		},
	})
	worker := server.NewWorker("test_worker", 0)
	errorChan := make(chan error)
	worker.LaunchAsync(errorChan)

	testAll(server, t)

	before := time.Now()
	worker.Quit()
	after := time.Now()

	d := after.Sub(before)
	b := math.Ceil(d.Seconds())

	// Including other time, so round up
	assert.Equal(t, b, float64(normalTasksPollPeriod+1))
}
