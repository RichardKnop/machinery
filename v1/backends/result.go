package backends

import (
	"errors"
	"reflect"

	"github.com/RichardKnop/machinery/v1/utils"
)

// AsyncResult represents a task result
type AsyncResult struct {
	taskUUID  string
	taskState *TaskState
	backend   Backend
}

// ChainAsyncResult represents a result of a chain of tasks
type ChainAsyncResult struct {
	asyncResults []*AsyncResult
	backend      Backend
}

// NewAsyncResult creates AsyncResult instance
func NewAsyncResult(taskUUID string, backend Backend) *AsyncResult {
	return &AsyncResult{
		taskUUID:  taskUUID,
		taskState: &TaskState{},
		backend:   backend,
	}
}

// NewChainAsyncResult creates ChainAsyncResult instance
func NewChainAsyncResult(taskUUIDs []string, backend Backend) *ChainAsyncResult {
	asyncResults := make([]*AsyncResult, len(taskUUIDs))
	for i, taskUUID := range taskUUIDs {
		asyncResults[i] = NewAsyncResult(taskUUID, backend)
	}
	return &ChainAsyncResult{
		asyncResults: asyncResults,
		backend:      backend,
	}
}

// Get returns task result (synchronous blocking call)
func (asyncResult *AsyncResult) Get() (reflect.Value, error) {
	if asyncResult.backend == nil {
		return reflect.Value{}, errors.New("Result backend not configured")
	}

	for {
		asyncResult.GetState()

		if asyncResult.taskState.IsSuccess() {
			return utils.ReflectValue(
				asyncResult.taskState.Result.Type,
				asyncResult.taskState.Result.Value,
			)
		}

		if asyncResult.taskState.IsFailure() {
			return reflect.Value{}, errors.New(asyncResult.taskState.Error)
		}
	}
}

// GetState returns latest task state
func (asyncResult *AsyncResult) GetState() *TaskState {
	if asyncResult.taskState.IsCompleted() {
		return asyncResult.taskState
	}

	taskState, err := asyncResult.backend.GetState(asyncResult.taskUUID)
	if err == nil {
		asyncResult.taskState = taskState
	}

	return asyncResult.taskState
}

// Get returns result of a chain of tasks (synchronous blocking call)
func (chainAsyncResult *ChainAsyncResult) Get() (reflect.Value, error) {
	if chainAsyncResult.backend == nil {
		return reflect.Value{}, errors.New("Result backend not configured")
	}

	var result reflect.Value
	var err error

	for _, asyncResult := range chainAsyncResult.asyncResults {
		result, err = asyncResult.Get()
		if err != nil {
			return result, err
		}
	}

	return result, err
}
