package backends_test

import (
	"os"
	"testing"
	"time"

	"github.com/vamsu/machinery/v1/backends"
	"github.com/vamsu/machinery/v1/config"
	"github.com/vamsu/machinery/v1/signatures"
	"github.com/stretchr/testify/assert"
)

func TestGroupCompletedRedis(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisURL == "" {
		return
	}

	groupUUID := "testGroupUUID"
	task1 := &signatures.TaskSignature{
		UUID:      "testTaskUUID1",
		GroupUUID: groupUUID,
	}
	task2 := &signatures.TaskSignature{
		UUID:      "testTaskUUID2",
		GroupUUID: groupUUID,
	}

	backend := backends.NewRedisBackend(&config.Config{}, redisURL, redisPassword, "", 0)

	// Cleanup before the test
	backend.PurgeState(task1.UUID)
	backend.PurgeState(task2.UUID)
	backend.PurgeGroupMeta(groupUUID)

	groupCompleted, err := backend.GroupCompleted(groupUUID, 2)
	if assert.Error(t, err) {
		assert.False(t, groupCompleted)
		assert.Equal(t, "redigo: nil returned", err.Error())
	}

	backend.InitGroup(groupUUID, []string{task1.UUID, task2.UUID})

	groupCompleted, err = backend.GroupCompleted(groupUUID, 2)
	if assert.Error(t, err) {
		assert.False(t, groupCompleted)
		assert.Equal(t, "Expected byte array, instead got: <nil>", err.Error())
	}

	backend.SetStatePending(task1)
	backend.SetStateStarted(task2)
	groupCompleted, err = backend.GroupCompleted(groupUUID, 2)
	if assert.NoError(t, err) {
		assert.False(t, groupCompleted)
	}

	backend.SetStateStarted(task1)
	backend.SetStateSuccess(task2, new(backends.TaskResult))
	groupCompleted, err = backend.GroupCompleted(groupUUID, 2)
	if assert.NoError(t, err) {
		assert.False(t, groupCompleted)
	}

	backend.SetStateFailure(task1, "Some error")
	groupCompleted, err = backend.GroupCompleted(groupUUID, 2)
	if assert.NoError(t, err) {
		assert.True(t, groupCompleted)
	}
}

func TestGetStateRedis(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisURL == "" {
		return
	}

	signature := &signatures.TaskSignature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend := backends.NewRedisBackend(new(config.Config), redisURL, redisPassword, "", 0)

	go func() {
		backend.SetStatePending(signature)
		<-time.After(2 * time.Millisecond)
		backend.SetStateReceived(signature)
		<-time.After(2 * time.Millisecond)
		backend.SetStateStarted(signature)
		<-time.After(2 * time.Millisecond)
		taskResult := &backends.TaskResult{
			Type:  "float64",
			Value: 2,
		}
		backend.SetStateSuccess(signature, taskResult)
	}()

	var (
		taskState *backends.TaskState
		err       error
	)
	for {
		taskState, err = backend.GetState(signature.UUID)
		if taskState == nil {
			assert.Equal(t, "redigo: nil returned", err.Error())
			continue
		}

		assert.NoError(t, err)
		if taskState.IsCompleted() {
			break
		}
	}
}

func TestPurgeStateRedis(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisURL == "" {
		return
	}

	signature := &signatures.TaskSignature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend := backends.NewRedisBackend(new(config.Config), redisURL, redisPassword, "", 0)

	backend.SetStatePending(signature)
	taskState, err := backend.GetState(signature.UUID)
	assert.NotNil(t, taskState)
	assert.NoError(t, err)

	backend.PurgeState(taskState.TaskUUID)
	taskState, err = backend.GetState(signature.UUID)
	assert.Nil(t, taskState)
	assert.Error(t, err)
}
