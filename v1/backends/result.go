package backends

import (
	"errors"
	"reflect"
	"time"

	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/RichardKnop/machinery/v1/utils"
)

// AsyncResult represents a task result
type AsyncResult struct {
	Signature *signatures.TaskSignature
	taskState *TaskState
	backend   Backend
}

// ChordAsyncResult represents a result of a chord
type ChordAsyncResult struct {
	groupAsyncResults []*AsyncResult
	chordAsyncResult  *AsyncResult
	backend           Backend
}

// ChainAsyncResult represents a result of a chain of tasks
type ChainAsyncResult struct {
	asyncResults []*AsyncResult
	backend      Backend
}

// NewAsyncResult creates AsyncResult instance
func NewAsyncResult(signature *signatures.TaskSignature, backend Backend) *AsyncResult {
	return &AsyncResult{
		Signature: signature,
		taskState: &TaskState{},
		backend:   backend,
	}
}

// NewChordAsyncResult creates ChordAsyncResult instance
func NewChordAsyncResult(groupTasks []*signatures.TaskSignature, chordCallback *signatures.TaskSignature, backend Backend) *ChordAsyncResult {
	asyncResults := make([]*AsyncResult, len(groupTasks))
	for i, task := range groupTasks {
		asyncResults[i] = NewAsyncResult(task, backend)
	}
	return &ChordAsyncResult{
		groupAsyncResults: asyncResults,
		chordAsyncResult:  NewAsyncResult(chordCallback, backend),
		backend:           backend,
	}
}

// NewChainAsyncResult creates ChainAsyncResult instance
func NewChainAsyncResult(tasks []*signatures.TaskSignature, backend Backend) *ChainAsyncResult {
	asyncResults := make([]*AsyncResult, len(tasks))
	for i, task := range tasks {
		asyncResults[i] = NewAsyncResult(task, backend)
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

		// Purge state if we are using AMQP backend
		_, isAMQPBackend := asyncResult.backend.(*AMQPBackend)
		if isAMQPBackend && asyncResult.taskState.IsCompleted() {
			asyncResult.backend.PurgeState(asyncResult.taskState.TaskUUID)
		}

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

// Get returns task result limited in time(synchronous blocking call)
func (asyncResult *AsyncResult) GetWithTimeout(timeoutD, sleepD time.Duration) (reflect.Value, error) {
	if asyncResult.backend == nil {
		return reflect.Value{}, errors.New("Result backend not configured")
	}

	timeout := time.NewTimer(timeoutD)

	for {
		select {
		case <-timeout.C:
			return reflect.Value{}, errors.New("Timeout reached")
		default:
			asyncResult.GetState()

			// Purge state if we are using AMQP backend
			_, isAMQPBackend := asyncResult.backend.(*AMQPBackend)
			if isAMQPBackend && asyncResult.taskState.IsCompleted() {
				asyncResult.backend.PurgeState(asyncResult.taskState.TaskUUID)
			}

			if asyncResult.taskState.IsSuccess() {
				return utils.ReflectValue(
					asyncResult.taskState.Result.Type,
					asyncResult.taskState.Result.Value,
				)
			}

			if asyncResult.taskState.IsFailure() {
				return reflect.Value{}, errors.New(asyncResult.taskState.Error)
			}
			time.Sleep(sleepD)
		}
	}
}

// GetState returns latest task state
func (asyncResult *AsyncResult) GetState() *TaskState {
	if asyncResult.taskState.IsCompleted() {
		return asyncResult.taskState
	}

	taskState, err := asyncResult.backend.GetState(asyncResult.Signature.UUID)
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

	var (
		result reflect.Value
		err    error
	)

	for _, asyncResult := range chainAsyncResult.asyncResults {
		result, err = asyncResult.Get()
		if err != nil {
			return result, err
		}
	}

	return result, err
}

// Get returns result of a chord (synchronous blocking call)
func (chordAsyncResult *ChordAsyncResult) Get() (reflect.Value, error) {
	if chordAsyncResult.backend == nil {
		return reflect.Value{}, errors.New("Result backend not configured")
	}

	var (
		result reflect.Value
		err    error
	)

	for _, asyncResult := range chordAsyncResult.groupAsyncResults {
		result, err = asyncResult.Get()
		if err != nil {
			return result, err
		}
	}

	return chordAsyncResult.chordAsyncResult.Get()
}
