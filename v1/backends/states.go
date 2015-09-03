package backends

import (
	"github.com/RichardKnop/machinery/v1/signatures"
)

const (
	// PendingState - initial state of a task
	PendingState = "PENDING"
	// ReceivedState - when task is received by a worker
	ReceivedState = "RECEIVED"
	// StartedState - when the worker starts processing the task
	StartedState = "STARTED"
	// SuccessState - when the task is processed successfully
	SuccessState = "SUCCESS"
	// FailureState - when processing of the task fails
	FailureState = "FAILURE"
)

// TaskResult represents an actual return value of a processed task
type TaskResult struct {
	Type  string
	Value interface{}
}

// TaskState represents a state of a task
type TaskState struct {
	TaskUUID string
	State    string
	Result   *TaskResult
	Error    string
}

// GroupMeta stores useful metadata about tasks within the same group
// E.g. UUIDs of all tasks which are used in order to check if all tasks
// completed successfully or not and thus whether to trigger chord callback
type GroupMeta struct {
	GroupUUID string
	TaskUUIDs []string
}

// NewPendingTaskState ...
func NewPendingTaskState(signature *signatures.TaskSignature) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    PendingState,
	}
}

// NewReceivedTaskState ...
func NewReceivedTaskState(signature *signatures.TaskSignature) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    ReceivedState,
	}
}

// NewStartedTaskState ...
func NewStartedTaskState(signature *signatures.TaskSignature) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    StartedState,
	}
}

// NewSuccessTaskState ...
func NewSuccessTaskState(signature *signatures.TaskSignature, result *TaskResult) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    SuccessState,
		Result:   result,
	}
}

// NewFailureTaskState ...
func NewFailureTaskState(signature *signatures.TaskSignature, err string) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    FailureState,
		Error:    err,
	}
}

// IsCompleted returns true if state is SUCCESSS or FAILURE,
// i.e. the task has finished processing and either succeeded or failed.
func (taskState *TaskState) IsCompleted() bool {
	return taskState.IsSuccess() || taskState.IsFailure()
}

// IsSuccess returns true if state is SUCCESSS
func (taskState *TaskState) IsSuccess() bool {
	return taskState.State == SuccessState
}

// IsFailure returns true if state is FAILURE
func (taskState *TaskState) IsFailure() bool {
	return taskState.State == FailureState
}
