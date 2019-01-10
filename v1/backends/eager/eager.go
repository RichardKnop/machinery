package eager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// ErrGroupNotFound ...
type ErrGroupNotFound struct {
	groupUUID string
}

// NewErrGroupNotFound returns new instance of ErrGroupNotFound
func NewErrGroupNotFound(groupUUID string) ErrGroupNotFound {
	return ErrGroupNotFound{groupUUID: groupUUID}
}

// Error implements error interface
func (e ErrGroupNotFound) Error() string {
	return fmt.Sprintf("Group not found: %v", e.groupUUID)
}

// ErrTasknotFound ...
type ErrTasknotFound struct {
	taskUUID string
}

// NewErrTasknotFound returns new instance of ErrTasknotFound
func NewErrTasknotFound(taskUUID string) ErrTasknotFound {
	return ErrTasknotFound{taskUUID: taskUUID}
}

// Error implements error interface
func (e ErrTasknotFound) Error() string {
	return fmt.Sprintf("Task not found: %v", e.taskUUID)
}

// Backend represents an "eager" in-memory result backend
type Backend struct {
	common.Backend
	groups     map[string][]string
	tasks      map[string][]byte
	stateMutex sync.Mutex
}

// New creates EagerBackend instance
func New() iface.Backend {
	return &Backend{
		Backend: common.NewBackend(new(config.Config)),
		groups:  make(map[string][]string),
		tasks:   make(map[string][]byte),
	}
}

// InitGroup creates and saves a group meta data object
func (b *Backend) InitGroup(groupUUID string, taskUUIDs []string) error {
	tasks := make([]string, 0, len(taskUUIDs))
	// copy every task
	for _, v := range taskUUIDs {
		tasks = append(tasks, v)
	}

	b.groups[groupUUID] = tasks
	return nil
}

// GroupCompleted returns true if all tasks in a group finished
func (b *Backend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	tasks, ok := b.groups[groupUUID]
	if !ok {
		return false, NewErrGroupNotFound(groupUUID)
	}

	var countSuccessTasks = 0
	for _, v := range tasks {
		t, err := b.GetState(v)
		if err != nil {
			return false, err
		}

		if t.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *Backend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	taskUUIDs, ok := b.groups[groupUUID]
	if !ok {
		return nil, NewErrGroupNotFound(groupUUID)
	}

	ret := make([]*tasks.TaskState, 0, groupTaskCount)
	for _, taskUUID := range taskUUIDs {
		t, err := b.GetState(taskUUID)
		if err != nil {
			return nil, err
		}

		ret = append(ret, t)
	}

	return ret, nil
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *Backend) TriggerChord(groupUUID string) (bool, error) {
	return true, nil
}

// SetStatePending updates task state to PENDING
func (b *Backend) SetStatePending(signature *tasks.Signature) error {
	state := tasks.NewPendingTaskState(signature)
	return b.updateState(state)
}

// SetStateReceived updates task state to RECEIVED
func (b *Backend) SetStateReceived(signature *tasks.Signature) error {
	state := tasks.NewReceivedTaskState(signature)
	return b.updateState(state)
}

// SetStateStarted updates task state to STARTED
func (b *Backend) SetStateStarted(signature *tasks.Signature) error {
	state := tasks.NewStartedTaskState(signature)
	return b.updateState(state)
}

// SetStateRetry updates task state to RETRY
func (b *Backend) SetStateRetry(signature *tasks.Signature) error {
	state := tasks.NewRetryTaskState(signature)
	return b.updateState(state)
}

// SetStateSuccess updates task state to SUCCESS
func (b *Backend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	state := tasks.NewSuccessTaskState(signature, results)
	return b.updateState(state)
}

// SetStateFailure updates task state to FAILURE
func (b *Backend) SetStateFailure(signature *tasks.Signature, err string) error {
	state := tasks.NewFailureTaskState(signature, err)
	return b.updateState(state)
}

// GetState returns the latest task state
func (b *Backend) GetState(taskUUID string) (*tasks.TaskState, error) {
	tasktStateBytes, ok := b.tasks[taskUUID]
	if !ok {
		return nil, NewErrTasknotFound(taskUUID)
	}

	state := new(tasks.TaskState)
	decoder := json.NewDecoder(bytes.NewReader(tasktStateBytes))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, fmt.Errorf("Failed to unmarshal task state %v", b)
	}

	return state, nil
}

// PurgeState deletes stored task state
func (b *Backend) PurgeState(taskUUID string) error {
	_, ok := b.tasks[taskUUID]
	if !ok {
		return NewErrTasknotFound(taskUUID)
	}

	delete(b.tasks, taskUUID)
	return nil
}

// PurgeGroupMeta deletes stored group meta data
func (b *Backend) PurgeGroupMeta(groupUUID string) error {
	_, ok := b.groups[groupUUID]
	if !ok {
		return NewErrGroupNotFound(groupUUID)
	}

	delete(b.groups, groupUUID)
	return nil
}

func (b *Backend) updateState(s *tasks.TaskState) error {
	// simulate the behavior of json marshal/unmarshal
	b.stateMutex.Lock()
	defer b.stateMutex.Unlock()
	msg, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("Marshal task state error: %v", err)
	}

	b.tasks[s.TaskUUID] = msg
	return nil
}
