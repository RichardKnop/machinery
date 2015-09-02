package backends

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/RichardKnop/machinery/Godeps/_workspace/src/github.com/bradfitz/gomemcache/memcache"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
)

// MemcacheBackend represents a Memcache result backend
type MemcacheBackend struct {
	config  *config.Config
	servers []string
	client  *memcache.Client
}

// NewMemcacheBackend creates MemcacheBackend instance
func NewMemcacheBackend(cnf *config.Config, servers []string) Backend {
	return Backend(&MemcacheBackend{
		config:  cnf,
		servers: servers,
	})
}

// SetStatePending - sets task state to PENDING
func (memcacheBackend *MemcacheBackend) SetStatePending(signature *signatures.TaskSignature) error {
	taskState := NewPendingTaskState(signature)

	if err := memcacheBackend.updateState(taskState); err != nil {
		return err
	}

	if signature.GroupUUID == "" {
		return nil
	}

	_, err := memcacheBackend.updateStateGroup(
		signature.GroupUUID,
		signature.GroupTaskCount,
		signature.UUID,
	)
	return err
}

// SetStateReceived - sets task state to RECEIVED
func (memcacheBackend *MemcacheBackend) SetStateReceived(signature *signatures.TaskSignature) error {
	taskState := NewReceivedTaskState(signature)

	if err := memcacheBackend.updateState(taskState); err != nil {
		return err
	}

	if signature.GroupUUID == "" {
		return nil
	}

	_, err := memcacheBackend.updateStateGroup(
		signature.GroupUUID,
		signature.GroupTaskCount,
		signature.UUID,
	)
	return err
}

// SetStateStarted - sets task state to STARTED
func (memcacheBackend *MemcacheBackend) SetStateStarted(signature *signatures.TaskSignature) error {
	taskState := NewStartedTaskState(signature)

	if err := memcacheBackend.updateState(taskState); err != nil {
		return err
	}

	if signature.GroupUUID == "" {
		return nil
	}

	_, err := memcacheBackend.updateStateGroup(
		signature.GroupUUID,
		signature.GroupTaskCount,
		signature.UUID,
	)
	return err
}

// SetStateSuccess - sets task state to SUCCESS
func (memcacheBackend *MemcacheBackend) SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) (*TaskStateGroup, error) {
	taskState := NewSuccessTaskState(signature, result)

	if err := memcacheBackend.updateState(taskState); err != nil {
		return nil, err
	}

	if signature.GroupUUID == "" {
		return nil, nil
	}

	return memcacheBackend.updateStateGroup(
		signature.GroupUUID,
		signature.GroupTaskCount,
		signature.UUID,
	)
}

// SetStateFailure - sets task state to FAILURE
func (memcacheBackend *MemcacheBackend) SetStateFailure(signature *signatures.TaskSignature, err string) error {
	taskState := NewFailureTaskState(signature, err)

	if err := memcacheBackend.updateState(taskState); err != nil {
		return err
	}

	if signature.GroupUUID == "" {
		return nil
	}

	_, errr := memcacheBackend.updateStateGroup(
		signature.GroupUUID,
		signature.GroupTaskCount,
		signature.UUID,
	)
	return errr
}

// GetState returns the latest task state
func (memcacheBackend *MemcacheBackend) GetState(taskUUID string) (*TaskState, error) {
	taskState := TaskState{}

	item, err := memcacheBackend.getClient().Get(taskUUID)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(item.Value, &taskState); err != nil {
		return nil, err
	}

	return &taskState, nil
}

// PurgeState - deletes stored task state
func (memcacheBackend *MemcacheBackend) PurgeState(taskState *TaskState) error {
	return memcacheBackend.getClient().Delete(taskState.TaskUUID)
}

// PurgeStateGroup - deletes stored task state
func (memcacheBackend *MemcacheBackend) PurgeStateGroup(taskStateGroup *TaskStateGroup) error {
	return memcacheBackend.getClient().Delete(taskStateGroup.GroupUUID)
}

// Updates a task state
func (memcacheBackend *MemcacheBackend) updateState(taskState *TaskState) error {
	encoded, err := json.Marshal(&taskState)
	if err != nil {
		return err
	}

	item, err := memcacheBackend.getClient().Get(taskState.TaskUUID)
	if err != nil {
		if err := memcacheBackend.getClient().Set(&memcache.Item{
			Key:   taskState.TaskUUID,
			Value: encoded,
		}); err != nil {
			return err
		}
	} else {
		item.Value = encoded
		memcacheBackend.getClient().Replace(item)
	}

	return memcacheBackend.setExpirationTime(taskState.TaskUUID)
}

// Updates a task state group
func (memcacheBackend *MemcacheBackend) updateStateGroup(groupUUID string, groupTaskCount int, taskUUID string) (*TaskStateGroup, error) {
	if groupUUID == "" || groupTaskCount == 0 {
		return nil, nil
	}

	var taskStateGroup *TaskStateGroup

	item, err := memcacheBackend.getClient().Get(groupUUID)
	if err != nil {
		taskStateGroup = &TaskStateGroup{
			GroupUUID:      groupUUID,
			GroupTaskCount: groupTaskCount,
			States:         make(map[string]*TaskState),
		}
	} else {
		if err := json.Unmarshal(item.Value, &taskStateGroup); err != nil {
			return nil, err
		}
	}

	taskState, err := memcacheBackend.GetState(taskUUID)
	if err != nil {
		return nil, err
	}
	taskStateGroup.States[taskUUID] = taskState

	// Due to asynchronous nature of task processing, a different task's state
	// might have changed while updating the task state group
	// Therefor we fetch correct states from the backend all the time
	for uuid := range taskStateGroup.States {
		if uuid == taskUUID {
			continue
		}
		taskState, err := memcacheBackend.GetState(uuid)
		if err != nil {
			return nil, err
		}
		taskStateGroup.States[uuid] = taskState
	}

	encoded, err := json.Marshal(taskStateGroup)
	if err != nil {
		return nil, fmt.Errorf("JSON Encode Message: %v", err)
	}

	if err := memcacheBackend.getClient().Set(&memcache.Item{
		Key:   groupUUID,
		Value: encoded,
	}); err != nil {
		return nil, err
	}

	return taskStateGroup, memcacheBackend.setExpirationTime(groupUUID)
}

// Sets expiration timestamp on a stored state
func (memcacheBackend *MemcacheBackend) setExpirationTime(key string) error {
	expiresIn := memcacheBackend.config.ResultsExpireIn
	if expiresIn == 0 {
		// // expire results after 1 hour by default
		expiresIn = 3600
	}
	expirationTimestamp := int32(time.Now().Unix() + int64(expiresIn))

	if err := memcacheBackend.getClient().Touch(
		key,
		expirationTimestamp,
	); err != nil {
		return err
	}

	return nil
}

// Returns / creates instance of Memcache client
func (memcacheBackend *MemcacheBackend) getClient() *memcache.Client {
	if memcacheBackend.client == nil {
		memcacheBackend.client = memcache.New(memcacheBackend.servers...)
	}
	return memcacheBackend.client
}
