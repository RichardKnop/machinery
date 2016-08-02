package backends_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/backends"
	"github.com/stretchr/testify/assert"
)

func TestTaskStateIsCompleted(t *testing.T) {
	taskState := &backends.TaskState{
		TaskUUID: "taskUUID",
		State:    backends.PendingState,
	}

	assert.False(t, taskState.IsCompleted())

	taskState.State = backends.ReceivedState
	assert.False(t, taskState.IsCompleted())

	taskState.State = backends.StartedState
	assert.False(t, taskState.IsCompleted())

	taskState.State = backends.SuccessState
	assert.True(t, taskState.IsCompleted())

	taskState.State = backends.FailureState
	assert.True(t, taskState.IsCompleted())
}
