package tasks

const (
	// PendingState - initial state of a task
	PendingState = "PENDING"
	// ReceivedState - when task is received by a worker
	ReceivedState = "RECEIVED"
	// StartedState - when the worker starts processing the task
	StartedState = "STARTED"
	// RetryState - when failed task has been scheduled for retry
	RetryState = "RETRY"
	// SuccessState - when the task is processed successfully
	SuccessState = "SUCCESS"
	// FailureState - when processing of the task fails
	FailureState = "FAILURE"
)

// TaskState represents a state of a task
type TaskState struct {
	TaskUUID string        `bson:"_id"`
	State    string        `bson:"state"`
	Results  []*TaskResult `bson:"results"`
	Error    string        `bson:"error"`
}

// GroupMeta stores useful metadata about tasks within the same group
// E.g. UUIDs of all tasks which are used in order to check if all tasks
// completed successfully or not and thus whether to trigger chord callback
type GroupMeta struct {
	GroupUUID      string   `bson:"_id"`
	TaskUUIDs      []string `bson:"task_uuids"`
	ChordTriggered bool     `bson:"chord_trigerred"`
	Lock           bool     `bson:"lock"`
}

// NewPendingTaskState ...
func NewPendingTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    PendingState,
	}
}

// NewReceivedTaskState ...
func NewReceivedTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    ReceivedState,
	}
}

// NewStartedTaskState ...
func NewStartedTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    StartedState,
	}
}

// NewSuccessTaskState ...
func NewSuccessTaskState(signature *Signature, results []*TaskResult) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    SuccessState,
		Results:  results,
	}
}

// NewFailureTaskState ...
func NewFailureTaskState(signature *Signature, err string) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    FailureState,
		Error:    err,
	}
}

// NewRetryTaskState ...
func NewRetryTaskState(signature *Signature) *TaskState {
	return &TaskState{
		TaskUUID: signature.UUID,
		State:    RetryState,
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
