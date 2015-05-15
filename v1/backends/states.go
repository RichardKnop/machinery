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
	Error    error
}
