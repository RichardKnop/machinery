package backends

import (
	"encoding/json"
	"fmt"

	"github.com/RichardKnop/machinery/v1/signatures"
)

// EagerBackend represents an "eager" in-memory result backend
type EagerBackend struct {
	groups map[string][]string
	tasks  map[string][]byte
}

// NewEagerBackend creates EagerBackend instance
func NewEagerBackend() Backend {
	return Backend(&EagerBackend{
		groups: make(map[string][]string),
		tasks:  make(map[string][]byte),
	})
}

// InitGroup - saves UUIDs of all tasks in a group
func (e *EagerBackend) InitGroup(groupUUID string, taskUUIDs []string) error {
	tasks := make([]string, 0, len(taskUUIDs))
	// copy every task
	for _, v := range taskUUIDs {
		tasks = append(tasks, v)
	}

	e.groups[groupUUID] = tasks
	return nil
}

// GroupCompleted - returns true if all tasks in a group finished
func (e *EagerBackend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	tasks, ok := e.groups[groupUUID]
	if !ok {
		return false, fmt.Errorf("Group not found: %v", groupUUID)
	}

	for _, v := range tasks {
		t, err := e.GetState(v)
		if err != nil {
			return false, err
		}

		if !t.IsCompleted() {
			return false, nil
		}
	}

	return true, nil
}

// GroupTaskStates - returns states of all tasks in the group
func (e *EagerBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*TaskState, error) {
	tasks, ok := e.groups[groupUUID]
	if !ok {
		return nil, fmt.Errorf("Group not found: %v", groupUUID)
	}

	ret := make([]*TaskState, 0, groupTaskCount)
	for _, v := range tasks {
		t, err := e.GetState(v)
		if err != nil {
			return nil, err
		}

		ret = append(ret, t)
	}

	return ret, nil
}

// SetStatePending - sets task state to PENDING
func (e *EagerBackend) SetStatePending(signature *signatures.TaskSignature) error {
	state := NewPendingTaskState(signature)
	return e.updateState(state)
}

// SetStateReceived - sets task state to RECEIVED
func (e *EagerBackend) SetStateReceived(signature *signatures.TaskSignature) error {
	state := NewReceivedTaskState(signature)
	return e.updateState(state)
}

// SetStateStarted - sets task state to STARTED
func (e *EagerBackend) SetStateStarted(signature *signatures.TaskSignature) error {
	state := NewStartedTaskState(signature)
	return e.updateState(state)
}

// SetStateSuccess - sets task state to SUCCESS
func (e *EagerBackend) SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) error {
	state := NewSuccessTaskState(signature, result)
	return e.updateState(state)
}

// SetStateFailure - sets task state to FAILURE
func (e *EagerBackend) SetStateFailure(signature *signatures.TaskSignature, err string) error {
	state := NewFailureTaskState(signature, err)
	return e.updateState(state)
}

// GetState - returns the latest task state
func (e *EagerBackend) GetState(taskUUID string) (*TaskState, error) {
	b, ok := e.tasks[taskUUID]
	if !ok {
		return nil, fmt.Errorf("Task not found: %v", taskUUID)
	}

	taskState := new(TaskState)
	err := json.Unmarshal(b, taskState)
	if err != nil {
		return nil, fmt.Errorf("Failed to unmarshal task state %v", b)
	}

	return taskState, nil
}

// PurgeState - deletes stored task state
func (e *EagerBackend) PurgeState(taskUUID string) error {
	_, ok := e.tasks[taskUUID]
	if !ok {
		return fmt.Errorf("Task not found: %v", taskUUID)
	}

	delete(e.tasks, taskUUID)
	return nil
}

// PurgeGroupMeta - deletes stored group meta data
func (e *EagerBackend) PurgeGroupMeta(groupUUID string) error {
	_, ok := e.groups[groupUUID]
	if !ok {
		return fmt.Errorf("Group not found: %v", groupUUID)
	}

	delete(e.groups, groupUUID)
	return nil
}

func (e *EagerBackend) updateState(s *TaskState) error {
	// simulate the behavior of json marshal/unmarshal
	msg, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("JSON Encode State: %v", err)
	}

	e.tasks[s.TaskUUID] = msg
	return nil
}
