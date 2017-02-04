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
func (b *MemcacheBackend) InitGroup(groupUUID string, taskUUIDs []string) error {
	groupMeta := &GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
	}

	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return err
	}

	return b.getClient().Set(&memcache.Item{
		Key:        groupUUID,
		Value:      encoded,
		Expiration: b.getExpirationTimestamp(),
	})
}

// GroupCompleted - returns true if all tasks in a group finished
func (b *MemcacheBackend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	for _, taskUUID := range groupMeta.TaskUUIDs {
		taskState, err := b.GetState(taskUUID)
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
func (b *MemcacheBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*TaskState, error) {
	taskStates := make([]*TaskState, groupTaskCount)

	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return taskStates, err
	}

	for i, taskUUID := range groupMeta.TaskUUIDs {
		taskState, err := b.GetState(taskUUID)
		if err != nil {
			return taskStates, err
		}

		taskStates[i] = taskState
	}

	return taskStates, nil
}

// TriggerChord - marks chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *MemcacheBackend) TriggerChord(groupUUID string) (bool, error) {
	// TODO - to be implemented, we will need a memcache distributed lock solution
	return true, nil
}

// SetStatePending - sets task state to PENDING
func (b *MemcacheBackend) SetStatePending(signature *signatures.TaskSignature) error {
	taskState := NewPendingTaskState(signature)
	return b.updateState(taskState)
}

// SetStateReceived - sets task state to RECEIVED
func (b *MemcacheBackend) SetStateReceived(signature *signatures.TaskSignature) error {
	taskState := NewReceivedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateStarted - sets task state to STARTED
func (b *MemcacheBackend) SetStateStarted(signature *signatures.TaskSignature) error {
	taskState := NewStartedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateSuccess - sets task state to SUCCESS
func (b *MemcacheBackend) SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) error {
	taskState := NewSuccessTaskState(signature, result)
	return b.updateState(taskState)
}

// SetStateFailure - sets task state to FAILURE
func (b *MemcacheBackend) SetStateFailure(signature *signatures.TaskSignature, err string) error {
	taskState := NewFailureTaskState(signature, err)
	return b.updateState(taskState)
}

// GetState - returns the latest task state
func (b *MemcacheBackend) GetState(taskUUID string) (*TaskState, error) {
	taskState := new(TaskState)

	item, err := b.getClient().Get(taskUUID)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(item.Value, taskState); err != nil {
		return nil, err
	}

	return taskState, nil
}

// PurgeState - deletes stored task state
func (b *MemcacheBackend) PurgeState(taskUUID string) error {
	return b.getClient().Delete(taskUUID)
}

// PurgeGroupMeta - deletes stored group meta data
func (b *MemcacheBackend) PurgeGroupMeta(groupUUID string) error {
	return b.getClient().Delete(groupUUID)
}

// Updates a task state
func (b *MemcacheBackend) updateState(taskState *TaskState) error {
	encoded, err := json.Marshal(&taskState)
	if err != nil {
		return err
	}

	return b.getClient().Set(&memcache.Item{
		Key:        taskState.TaskUUID,
		Value:      encoded,
		Expiration: b.getExpirationTimestamp(),
	})
}

// Fetches GroupMeta from the backend, convenience function to avoid repetition
func (b *MemcacheBackend) getGroupMeta(groupUUID string) (*GroupMeta, error) {
	groupMeta := new(GroupMeta)

	item, err := b.getClient().Get(groupUUID)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(item.Value, groupMeta); err != nil {
		return nil, err
	}

	return groupMeta, nil
}

// Returns expiration timestamp
func (b *MemcacheBackend) getExpirationTimestamp() int32 {
	expiresIn := b.config.ResultsExpireIn
	if expiresIn == 0 {
		// // expire results after 1 hour by default
		expiresIn = 3600
	}
	return int32(time.Now().Unix() + int64(expiresIn))
}

// Returns / creates instance of Memcache client
func (b *MemcacheBackend) getClient() *memcache.Client {
	if b.client == nil {
		b.client = memcache.New(b.servers...)
	}
	return b.client
}
