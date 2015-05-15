package backends

import (
	"reflect"

	"github.com/RichardKnop/machinery/v1/utils"
)

// AsyncResult represents an asynchronous task result
type AsyncResult struct {
	taskUUID  string
	taskState *TaskState
	backend   Backend
}

// NewAsyncResult creates AsyncResult instance
func NewAsyncResult(taskUUID string, backend Backend) *AsyncResult {
	return &AsyncResult{
		taskUUID:  taskUUID,
		taskState: &TaskState{},
		backend:   backend,
	}
}

// Get returns task result (synchronous blocking call)
func (asyncResult *AsyncResult) Get() (reflect.Value, error) {
	for {
		asyncResult.GetState()

		if asyncResult.IsSuccess() {
			return utils.ReflectValue(
				asyncResult.taskState.Result.Type,
				asyncResult.taskState.Result.Value,
			)
		}

		if asyncResult.IsFailure() {
			return reflect.Value{}, asyncResult.taskState.Error
		}
	}
}

// GetState returns latest task state
func (asyncResult *AsyncResult) GetState() *TaskState {
	if asyncResult.IsCompleted() {
		return asyncResult.taskState
	}
	taskState, err := asyncResult.backend.GetState(asyncResult.taskUUID)
	if err == nil {
		asyncResult.taskState = taskState
	}
	return asyncResult.taskState
}

// IsCompleted returns true if state is SUCCESSS or FAILURE,
// i.e. the task has finished processing and either succeeded or failed.
func (asyncResult *AsyncResult) IsCompleted() bool {
	return asyncResult.IsSuccess() || asyncResult.IsFailure()
}

// IsSuccess returns true if state is SUCCESSS
func (asyncResult *AsyncResult) IsSuccess() bool {
	return asyncResult.taskState.State == SuccessState
}

// IsFailure returns true if state is FAILURE
func (asyncResult *AsyncResult) IsFailure() bool {
	return asyncResult.taskState.State == FailureState
}
