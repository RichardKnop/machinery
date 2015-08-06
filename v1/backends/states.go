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

// TaskStateGroup represents a state of group of tasks
type TaskStateGroup struct {
	GroupUUID      string
	GroupTaskCount int
	States         map[string]TaskState
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

// IsCompleted returns true if state of all tasks in a group
// is SUCCESS or FAILURE which means all grouped tasks are completed
func (taskStateGroup *TaskStateGroup) IsCompleted() bool {
	completedCount := 0
	for _, taskState := range taskStateGroup.States {
		if taskState.IsSuccess() || taskState.IsFailure() {
			completedCount++
		}
	}
	return completedCount == taskStateGroup.GroupTaskCount
}

// IsSuccess returns true if state of all grouped tasks is SUCCESSS
func (taskStateGroup *TaskStateGroup) IsSuccess() bool {
	successCount := 0
	for _, taskState := range taskStateGroup.States {
		if taskState.IsSuccess() {
			successCount++
		}
	}
	return successCount == taskStateGroup.GroupTaskCount
}

// IsFailure returns true if state of any single task in the group is FAILURE
func (taskStateGroup *TaskStateGroup) IsFailure() bool {
	failureCount := 0
	for _, taskState := range taskStateGroup.States {
		if taskState.IsFailure() {
			failureCount++
		}
	}
	return failureCount > 0
}
