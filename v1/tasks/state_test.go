package tasks_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestTaskStateIsCompleted(t *testing.T) {
	t.Parallel()

	taskState := &tasks.TaskState{
		TaskUUID: "taskUUID",
		State:    tasks.StatePending,
	}

	assert.False(t, taskState.IsCompleted())

	taskState.State = tasks.StateReceived
	assert.False(t, taskState.IsCompleted())

	taskState.State = tasks.StateStarted
	assert.False(t, taskState.IsCompleted())

	taskState.State = tasks.StateSuccess
	assert.True(t, taskState.IsCompleted())

	taskState.State = tasks.StateFailure
	assert.True(t, taskState.IsCompleted())
}
