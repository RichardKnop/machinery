package backends

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

// NewPendingTaskState ...
func NewPendingTaskState(taskUUID string) *TaskState {
	return &TaskState{
		TaskUUID: taskUUID,
		State:    PendingState,
	}
}

// NewReceivedTaskState ...
func NewReceivedTaskState(taskUUID string) *TaskState {
	return &TaskState{
		TaskUUID: taskUUID,
		State:    ReceivedState,
	}
}

// NewStartedTaskState ...
func NewStartedTaskState(taskUUID string) *TaskState {
	return &TaskState{
		TaskUUID: taskUUID,
		State:    StartedState,
	}
}

// NewSuccessTaskState ...
func NewSuccessTaskState(taskUUID string, result *TaskResult) *TaskState {
	return &TaskState{
		TaskUUID: taskUUID,
		State:    SuccessState,
		Result:   result,
	}
}

// NewFailureTaskState ...
func NewFailureTaskState(taskUUID string, err string) *TaskState {
	return &TaskState{
		TaskUUID: taskUUID,
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
