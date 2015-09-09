package backends

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/RichardKnop/machinery/v1/signatures"
)

type EagerBackend struct {
	groups map[string][]string
	tasks  map[string][]byte
}

func NewEagerBackend() Backend {
	return Backend(&EagerBackend{
		groups: make(map[string][]string),
		tasks:  make(map[string][]byte),
	})
}

//
// Backend interface
//

func (e *EagerBackend) InitGroup(groupUUID string, taskUUIDs []string) error {
	tasks := make([]string, 0, len(taskUUIDs))
	// copy every task
	for _, v := range taskUUIDs {
		tasks = append(tasks, v)
	}

	e.groups[groupUUID] = tasks
	return nil
}

func (e *EagerBackend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	tasks, ok := e.groups[groupUUID]
	if !ok {
		return false, errors.New(fmt.Sprintf("group not found: %v", groupUUID))
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

func (e *EagerBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*TaskState, error) {
	tasks, ok := e.groups[groupUUID]
	if !ok {
		return nil, errors.New(fmt.Sprintf("group not found: %v", groupUUID))
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

func (e *EagerBackend) SetStatePending(signature *signatures.TaskSignature) error {
	state := NewPendingTaskState(signature)
	return e.updateState(state)
}

func (e *EagerBackend) SetStateReceived(signature *signatures.TaskSignature) error {
	state := NewReceivedTaskState(signature)
	return e.updateState(state)
}

func (e *EagerBackend) SetStateStarted(signature *signatures.TaskSignature) error {
	state := NewStartedTaskState(signature)
	return e.updateState(state)
}

func (e *EagerBackend) SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) error {
	state := NewSuccessTaskState(signature, result)
	return e.updateState(state)
}

func (e *EagerBackend) SetStateFailure(signature *signatures.TaskSignature, err string) error {
	state := NewFailureTaskState(signature, err)
	return e.updateState(state)
}

func (e *EagerBackend) GetState(taskUUID string) (*TaskState, error) {
	b, ok := e.tasks[taskUUID]
	if !ok {
		return nil, errors.New(fmt.Sprintf("task not found: %v", taskUUID))
	}

	ret := &TaskState{}
	err := json.Unmarshal(b, ret)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("json unmarshal failed %v", b))
	}

	return ret, nil
}

func (e *EagerBackend) PurgeState(taskUUID string) error {
	_, ok := e.tasks[taskUUID]
	if !ok {
		return errors.New(fmt.Sprintf("task not found: %v", taskUUID))
	}

	delete(e.tasks, taskUUID)
	return nil
}

func (e *EagerBackend) PurgeGroupMeta(groupUUID string) error {
	_, ok := e.groups[groupUUID]
	if !ok {
		return errors.New(fmt.Sprintf("group not found: %v", groupUUID))
	}

	delete(e.groups, groupUUID)
	return nil
}

//
// internal function
//

func (e *EagerBackend) updateState(s *TaskState) error {
	// simulate the behavior of json marshal/unmarshal
	msg, err := json.Marshal(s)
	if err != nil {
		return errors.New(fmt.Sprintf("json marshal failed %v", s))
	}

	e.tasks[s.TaskUUID] = msg
	return nil
}
