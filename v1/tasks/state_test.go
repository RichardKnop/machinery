package tasks_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestTaskStateIsCompleted(t *testing.T) {
	taskState := &tasks.TaskState{
		TaskUUID: "taskUUID",
		State:    tasks.PendingState,
	}

	assert.False(t, taskState.IsCompleted())

	taskState.State = tasks.ReceivedState
	assert.False(t, taskState.IsCompleted())

	taskState.State = tasks.StartedState
	assert.False(t, taskState.IsCompleted())

	taskState.State = tasks.SuccessState
	assert.True(t, taskState.IsCompleted())

	taskState.State = tasks.FailureState
	assert.True(t, taskState.IsCompleted())
}
