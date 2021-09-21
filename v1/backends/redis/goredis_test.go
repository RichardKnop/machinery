package redis_test

import (
	"github.com/RichardKnop/machinery/v1/backends/iface"
	"os"
	"strings"
	"testing"

	"github.com/RichardKnop/machinery/v1/backends/redis"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func getRedisG() iface.Backend {
	// host1:port1,host2:port2
	redisURL := os.Getenv("REDIS_URL_GR")
	//redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisURL == "" {
		return nil
	}
	backend := redis.NewGR(new(config.Config), strings.Split(redisURL, ","), 0)
	return backend
}

func TestGroupCompletedGR(t *testing.T) {
	backend := getRedisG()
	if backend == nil {
		t.Skip()
	}

	groupUUID := "testGroupUUID"
	task1 := &tasks.Signature{
		UUID:      "testTaskUUID1",
		GroupUUID: groupUUID,
	}
	task2 := &tasks.Signature{
		UUID:      "testTaskUUID2",
		GroupUUID: groupUUID,
	}

	// Cleanup before the test
	backend.PurgeState(task1.UUID)
	backend.PurgeState(task2.UUID)
	backend.PurgeGroupMeta(groupUUID)

	groupCompleted, err := backend.GroupCompleted(groupUUID, 2)
	if assert.Error(t, err) {
		assert.False(t, groupCompleted)
		assert.Equal(t, "redis: nil", err.Error())
	}

	backend.InitGroup(groupUUID, []string{task1.UUID, task2.UUID})

	groupCompleted, err = backend.GroupCompleted(groupUUID, 2)
	if assert.Error(t, err) {
		assert.False(t, groupCompleted)
		assert.Equal(t, "redis: nil", err.Error())
	}

	backend.SetStatePending(task1)
	backend.SetStateStarted(task2)
	groupCompleted, err = backend.GroupCompleted(groupUUID, 2)
	if assert.NoError(t, err) {
		assert.False(t, groupCompleted)
	}

	taskResults := []*tasks.TaskResult{new(tasks.TaskResult)}
	backend.SetStateStarted(task1)
	backend.SetStateSuccess(task2, taskResults)
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

func TestGetStateGR(t *testing.T) {
	backend := getRedisG()
	if backend == nil {
		t.Skip()
	}

	signature := &tasks.Signature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend.PurgeState("testTaskUUID")

	var (
		taskState *tasks.TaskState
		err       error
	)

	taskState, err = backend.GetState(signature.UUID)
	assert.Equal(t, "redis: nil", err.Error())
	assert.Nil(t, taskState)

	//Pending State
	backend.SetStatePending(signature)
	taskState, err = backend.GetState(signature.UUID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	createdAt := taskState.CreatedAt

	//Received State
	backend.SetStateReceived(signature)
	taskState, err = backend.GetState(signature.UUID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	assert.Equal(t, createdAt, taskState.CreatedAt)

	//Started State
	backend.SetStateStarted(signature)
	taskState, err = backend.GetState(signature.UUID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	assert.Equal(t, createdAt, taskState.CreatedAt)

	//Success State
	taskResults := []*tasks.TaskResult{
		{
			Type:  "float64",
			Value: 2,
		},
	}
	backend.SetStateSuccess(signature, taskResults)
	taskState, err = backend.GetState(signature.UUID)
	assert.NoError(t, err)
	assert.Equal(t, signature.Name, taskState.TaskName)
	assert.Equal(t, createdAt, taskState.CreatedAt)
	assert.NotNil(t, taskState.Results)
}

func TestPurgeStateGR(t *testing.T) {
	backend := getRedisG()
	if backend == nil {
		t.Skip()
	}

	signature := &tasks.Signature{
		UUID:      "testTaskUUID",
		GroupUUID: "testGroupUUID",
	}

	backend.SetStatePending(signature)
	taskState, err := backend.GetState(signature.UUID)
	assert.NotNil(t, taskState)
	assert.NoError(t, err)

	backend.PurgeState(taskState.TaskUUID)
	taskState, err = backend.GetState(signature.UUID)
	assert.Nil(t, taskState)
	assert.Error(t, err)
}
