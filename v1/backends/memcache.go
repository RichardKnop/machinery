package backends

import (
	"encoding/json"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/bradfitz/gomemcache/memcache"
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

// InitGroup - saves UUIDs of all tasks in a group
func (memcacheBackend *MemcacheBackend) InitGroup(groupUUID string, taskUUIDs []string) error {
	groupMeta := &GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
	}

	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return err
	}

	return memcacheBackend.getClient().Set(&memcache.Item{
		Key:        groupUUID,
		Value:      encoded,
		Expiration: memcacheBackend.getExpirationTimestamp(),
	})
}

// GroupCompleted - returns true if all tasks in a group finished
func (memcacheBackend *MemcacheBackend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta := new(GroupMeta)

	item, err := memcacheBackend.getClient().Get(groupUUID)
	if err != nil {
		return false, err
	}

	if err := json.Unmarshal(item.Value, groupMeta); err != nil {
		return false, err
	}

	for _, taskUUID := range groupMeta.TaskUUIDs {
		taskState, err := memcacheBackend.GetState(taskUUID)
		if err != nil {
			return false, err
		}

		if !taskState.IsCompleted() {
			return false, nil
		}
	}

	return true, nil
}

// GroupTaskStates - returns states of all tasks in the group
func (memcacheBackend *MemcacheBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*TaskState, error) {
	taskStates := make([]*TaskState, groupTaskCount)

	groupMeta := new(GroupMeta)

	item, err := memcacheBackend.getClient().Get(groupUUID)
	if err != nil {
		return taskStates, err
	}

	if err := json.Unmarshal(item.Value, groupMeta); err != nil {
		return taskStates, err
	}

	for i, taskUUID := range groupMeta.TaskUUIDs {
		taskState, err := memcacheBackend.GetState(taskUUID)
		if err != nil {
			return taskStates, err
		}

		taskStates[i] = taskState
	}

	return taskStates, nil
}

// SetStatePending - sets task state to PENDING
func (memcacheBackend *MemcacheBackend) SetStatePending(signature *signatures.TaskSignature) error {
	taskState := NewPendingTaskState(signature)
	return memcacheBackend.updateState(taskState)
}

// SetStateReceived - sets task state to RECEIVED
func (memcacheBackend *MemcacheBackend) SetStateReceived(signature *signatures.TaskSignature) error {
	taskState := NewReceivedTaskState(signature)
	return memcacheBackend.updateState(taskState)
}

// SetStateStarted - sets task state to STARTED
func (memcacheBackend *MemcacheBackend) SetStateStarted(signature *signatures.TaskSignature) error {
	taskState := NewStartedTaskState(signature)
	return memcacheBackend.updateState(taskState)
}

// SetStateSuccess - sets task state to SUCCESS
func (memcacheBackend *MemcacheBackend) SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) error {
	taskState := NewSuccessTaskState(signature, result)
	return memcacheBackend.updateState(taskState)
}

// SetStateFailure - sets task state to FAILURE
func (memcacheBackend *MemcacheBackend) SetStateFailure(signature *signatures.TaskSignature, err string) error {
	taskState := NewFailureTaskState(signature, err)
	return memcacheBackend.updateState(taskState)
}

// GetState - returns the latest task state
func (memcacheBackend *MemcacheBackend) GetState(taskUUID string) (*TaskState, error) {
	taskState := new(TaskState)

	item, err := memcacheBackend.getClient().Get(taskUUID)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(item.Value, taskState); err != nil {
		return nil, err
	}

	return taskState, nil
}

// PurgeState - deletes stored task state
func (memcacheBackend *MemcacheBackend) PurgeState(taskUUID string) error {
	return memcacheBackend.getClient().Delete(taskUUID)
}

// PurgeGroupMeta - deletes stored group meta data
func (memcacheBackend *MemcacheBackend) PurgeGroupMeta(groupUUID string) error {
	return memcacheBackend.getClient().Delete(groupUUID)
}

// Updates a task state
func (memcacheBackend *MemcacheBackend) updateState(taskState *TaskState) error {
	encoded, err := json.Marshal(&taskState)
	if err != nil {
		return err
	}

	return memcacheBackend.getClient().Set(&memcache.Item{
		Key:        taskState.TaskUUID,
		Value:      encoded,
		Expiration: memcacheBackend.getExpirationTimestamp(),
	})
}

// Returns expiration timestamp
func (memcacheBackend *MemcacheBackend) getExpirationTimestamp() int32 {
	expiresIn := memcacheBackend.config.ResultsExpireIn
	if expiresIn == 0 {
		// // expire results after 1 hour by default
		expiresIn = 3600
	}
	return int32(time.Now().Unix() + int64(expiresIn))
}

// Returns / creates instance of Memcache client
func (memcacheBackend *MemcacheBackend) getClient() *memcache.Client {
	if memcacheBackend.client == nil {
		memcacheBackend.client = memcache.New(memcacheBackend.servers...)
	}
	return memcacheBackend.client
}
