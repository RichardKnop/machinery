package backends

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/bradfitz/gomemcache/memcache"
)

// MemcacheBackend represents a Memcache result backend
type MemcacheBackend struct {
	config  *config.Config
	servers []string
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
		taskState,
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
		taskState,
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
		taskState,
	)
	return err
}

// SetStateSuccess - sets task state to SUCCESS
func (memcacheBackend *MemcacheBackend) SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) (*TaskStateGroup, error) {
	taskState := NewSuccessTaskState(signature, result)
	var taskStateGroup *TaskStateGroup

	if err := memcacheBackend.updateState(taskState); err != nil {
		return taskStateGroup, err
	}

	if signature.GroupUUID == "" {
		return taskStateGroup, nil
	}

	return memcacheBackend.updateStateGroup(
		signature.GroupUUID,
		signature.GroupTaskCount,
		taskState,
	)
}

// SetStateFailure - sets task state to FAILURE
func (memcacheBackend *MemcacheBackend) SetStateFailure(signature *signatures.TaskSignature, err string) (*TaskStateGroup, error) {
	taskState := NewFailureTaskState(signature, err)
	var taskStateGroup *TaskStateGroup

	if err := memcacheBackend.updateState(taskState); err != nil {
		return taskStateGroup, err
	}

	if signature.GroupUUID == "" {
		return taskStateGroup, nil
	}

	return memcacheBackend.updateStateGroup(
		signature.GroupUUID,
		signature.GroupTaskCount,
		taskState,
	)
}

// GetState returns the latest task state
func (memcacheBackend *MemcacheBackend) GetState(signature *signatures.TaskSignature) (*TaskState, error) {
	taskState := TaskState{}

	client := memcache.New(memcacheBackend.servers...)

	item, err := client.Get(signature.UUID)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(item.Value, &taskState); err != nil {
		return nil, err
	}

	return &taskState, nil
}

// GetStateGroup returns the latest task state group
func (memcacheBackend *MemcacheBackend) GetStateGroup(groupUUID string) (*TaskStateGroup, error) {
	taskStateGroup := TaskStateGroup{}

	client := memcache.New(memcacheBackend.servers...)

	item, err := client.Get(groupUUID)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(item.Value, &taskStateGroup); err != nil {
		return nil, err
	}

	return &taskStateGroup, nil
}

// PurgeState - deletes stored task state
func (memcacheBackend *MemcacheBackend) PurgeState(signature *signatures.TaskSignature) error {
	purgeUUIDs := []string{signature.UUID}
	if signature.GroupUUID != "" {
		purgeUUIDs = append(purgeUUIDs, signature.GroupUUID)
	}

	client := memcache.New(memcacheBackend.servers...)
	for _, purgeUUID := range purgeUUIDs {
		if err := client.Delete(purgeUUID); err != nil {
			return err
		}
	}

	return nil
}

// Updates a task state
func (memcacheBackend *MemcacheBackend) updateState(taskState *TaskState) error {
	encoded, err := json.Marshal(&taskState)
	if err != nil {
		return err
	}

	client := memcache.New(memcacheBackend.servers...)

	if err := client.Set(&memcache.Item{
		Key:   taskState.TaskUUID,
		Value: encoded,
	}); err != nil {
		return err
	}

	return memcacheBackend.setExpirationTime(taskState.TaskUUID)
}

// Updates a task state group
func (memcacheBackend *MemcacheBackend) updateStateGroup(groupUUID string, groupTaskCount int, taskState *TaskState) (*TaskStateGroup, error) {
	var taskStateGroup *TaskStateGroup

	client := memcache.New(memcacheBackend.servers...)

	item, err := client.Get(groupUUID)
	if err != nil {
		taskStateGroup = &TaskStateGroup{
			GroupUUID:      groupUUID,
			GroupTaskCount: groupTaskCount,
			States:         make(map[string]TaskState),
		}
	} else {
		if err := json.Unmarshal(item.Value, &taskStateGroup); err != nil {
			log.Printf("Failed to unmarshal task state group: %v", string(item.Value))
			log.Print(err)
			return taskStateGroup, err
		}
	}

	taskStateGroup.States[taskState.TaskUUID] = *taskState

	encoded, err := json.Marshal(taskStateGroup)
	if err != nil {
		return taskStateGroup, fmt.Errorf("JSON Encode Message: %v", err)
	}

	if err := client.Set(&memcache.Item{
		Key:   groupUUID,
		Value: encoded,
	}); err != nil {
		return taskStateGroup, err
	}

	return taskStateGroup, memcacheBackend.setExpirationTime(groupUUID)
}

// Sets expiration timestamp on a stored state
func (memcacheBackend *MemcacheBackend) setExpirationTime(key string) error {
	client := memcache.New(memcacheBackend.servers...)

	expiresIn := memcacheBackend.config.ResultsExpireIn
	if expiresIn == 0 {
		// // expire results after 1 hour by default
		expiresIn = 3600
	}
	expirationTimestamp := int32(time.Now().Unix() + int64(expiresIn))

	if err := client.Touch(
		key,
		expirationTimestamp,
	); err != nil {
		return err
	}

	return nil
}
