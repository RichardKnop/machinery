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
func (b *EagerBackend) InitGroup(groupUUID string, taskUUIDs []string) error {
	tasks := make([]string, 0, len(taskUUIDs))
	// copy every task
	for _, v := range taskUUIDs {
		tasks = append(tasks, v)
	}

	b.groups[groupUUID] = tasks
	return nil
}

// GroupCompleted - returns true if all tasks in a group finished
func (b *EagerBackend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	tasks, ok := b.groups[groupUUID]
	if !ok {
		return false, fmt.Errorf("Group not found: %v", groupUUID)
	}

	for _, v := range tasks {
		t, err := b.GetState(v)
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
func (b *EagerBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*TaskState, error) {
	tasks, ok := b.groups[groupUUID]
	if !ok {
		return nil, fmt.Errorf("Group not found: %v", groupUUID)
	}

	ret := make([]*TaskState, 0, groupTaskCount)
	for _, v := range tasks {
		t, err := b.GetState(v)
		if err != nil {
			return nil, err
		}

		ret = append(ret, t)
	}

	return ret, nil
}

// TriggerChord - marks chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *EagerBackend) TriggerChord(groupUUID string) (bool, error) {
	return true, nil
}

// SetStatePending - sets task state to PENDING
func (b *EagerBackend) SetStatePending(signature *signatures.TaskSignature) error {
	state := NewPendingTaskState(signature)
	return b.updateState(state)
}

// SetStateReceived - sets task state to RECEIVED
func (b *EagerBackend) SetStateReceived(signature *signatures.TaskSignature) error {
	state := NewReceivedTaskState(signature)
	return b.updateState(state)
}

// SetStateStarted - sets task state to STARTED
func (b *EagerBackend) SetStateStarted(signature *signatures.TaskSignature) error {
	state := NewStartedTaskState(signature)
	return b.updateState(state)
}

// SetStateSuccess - sets task state to SUCCESS
func (b *EagerBackend) SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) error {
	state := NewSuccessTaskState(signature, result)
	return b.updateState(state)
}

// SetStateFailure - sets task state to FAILURE
func (b *EagerBackend) SetStateFailure(signature *signatures.TaskSignature, err string) error {
	state := NewFailureTaskState(signature, err)
	return b.updateState(state)
}

// GetState - returns the latest task state
func (b *EagerBackend) GetState(taskUUID string) (*TaskState, error) {
	tasktStateBytes, ok := b.tasks[taskUUID]
	if !ok {
		return nil, fmt.Errorf("Task not found: %v", taskUUID)
	}

	taskState := new(TaskState)
	err := json.Unmarshal(tasktStateBytes, taskState)
	if err != nil {
		return nil, fmt.Errorf("Failed to unmarshal task state %v", b)
	}

	return taskState, nil
}

// PurgeState - deletes stored task state
func (b *EagerBackend) PurgeState(taskUUID string) error {
	_, ok := b.tasks[taskUUID]
	if !ok {
		return fmt.Errorf("Task not found: %v", taskUUID)
	}

	delete(b.tasks, taskUUID)
	return nil
}

// PurgeGroupMeta - deletes stored group meta data
func (b *EagerBackend) PurgeGroupMeta(groupUUID string) error {
	_, ok := b.groups[groupUUID]
	if !ok {
		return fmt.Errorf("Group not found: %v", groupUUID)
	}

	delete(b.groups, groupUUID)
	return nil
}

func (b *EagerBackend) updateState(s *TaskState) error {
	// simulate the behavior of json marshal/unmarshal
	msg, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("JSON Encode State: %v", err)
	}

	b.tasks[s.TaskUUID] = msg
	return nil
}
