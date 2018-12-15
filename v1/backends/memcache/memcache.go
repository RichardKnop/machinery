package memcache

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"

	gomemcache "github.com/bradfitz/gomemcache/memcache"
)

// Backend represents a Memcache result backend
type Backend struct {
	common.Backend
	servers []string
	client  *gomemcache.Client
}

// New creates Backend instance
func New(cnf *config.Config, servers []string) iface.Backend {
	return &Backend{
		Backend: common.NewBackend(cnf),
		servers: servers,
	}
}

// InitGroup creates and saves a group meta data object
func (b *Backend) InitGroup(groupUUID string, taskUUIDs []string) error {
	groupMeta := &tasks.GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
		CreatedAt: time.Now().UTC(),
	}

	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return err
	}

	return b.getClient().Set(&gomemcache.Item{
		Key:        groupUUID,
		Value:      encoded,
		Expiration: b.getExpirationTimestamp(),
	})
}

// GroupCompleted returns true if all tasks in a group finished
func (b *Backend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}

	var countSuccessTasks = 0
	for _, taskState := range taskStates {
		if taskState.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *Backend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return []*tasks.TaskState{}, err
	}

	return b.getStates(groupMeta.TaskUUIDs...)
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *Backend) TriggerChord(groupUUID string) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	// Chord has already been triggered, return false (should not trigger again)
	if groupMeta.ChordTriggered {
		return false, nil
	}

	// If group meta is locked, wait until it's unlocked
	for groupMeta.Lock {
		groupMeta, _ = b.getGroupMeta(groupUUID)
		log.WARNING.Print("Group meta locked, waiting")
		time.Sleep(time.Millisecond * 5)
	}

	// Acquire lock
	if err = b.lockGroupMeta(groupMeta); err != nil {
		return false, err
	}
	defer b.unlockGroupMeta(groupMeta)

	// Update the group meta data
	groupMeta.ChordTriggered = true
	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return false, err
	}
	if err = b.getClient().Replace(&gomemcache.Item{
		Key:        groupUUID,
		Value:      encoded,
		Expiration: b.getExpirationTimestamp(),
	}); err != nil {
		return false, err
	}

	return true, nil
}

// SetStatePending updates task state to PENDING
func (b *Backend) SetStatePending(signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	return b.updateState(taskState)
}

// SetStateReceived updates task state to RECEIVED
func (b *Backend) SetStateReceived(signature *tasks.Signature) error {
	taskState := tasks.NewReceivedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateStarted updates task state to STARTED
func (b *Backend) SetStateStarted(signature *tasks.Signature) error {
	taskState := tasks.NewStartedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateRetry updates task state to RETRY
func (b *Backend) SetStateRetry(signature *tasks.Signature) error {
	state := tasks.NewRetryTaskState(signature)
	return b.updateState(state)
}

// SetStateSuccess updates task state to SUCCESS
func (b *Backend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	taskState := tasks.NewSuccessTaskState(signature, results)
	return b.updateState(taskState)
}

// SetStateFailure updates task state to FAILURE
func (b *Backend) SetStateFailure(signature *tasks.Signature, err string) error {
	taskState := tasks.NewFailureTaskState(signature, err)
	return b.updateState(taskState)
}

// GetState returns the latest task state
func (b *Backend) GetState(taskUUID string) (*tasks.TaskState, error) {
	item, err := b.getClient().Get(taskUUID)
	if err != nil {
		return nil, err
	}

	state := new(tasks.TaskState)
	decoder := json.NewDecoder(bytes.NewReader(item.Value))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, err
	}

	return state, nil
}

// PurgeState deletes stored task state
func (b *Backend) PurgeState(taskUUID string) error {
	return b.getClient().Delete(taskUUID)
}

// PurgeGroupMeta deletes stored group meta data
func (b *Backend) PurgeGroupMeta(groupUUID string) error {
	return b.getClient().Delete(groupUUID)
}

// updateState saves current task state
func (b *Backend) updateState(taskState *tasks.TaskState) error {
	encoded, err := json.Marshal(taskState)
	if err != nil {
		return err
	}

	return b.getClient().Set(&gomemcache.Item{
		Key:        taskState.TaskUUID,
		Value:      encoded,
		Expiration: b.getExpirationTimestamp(),
	})
}

// lockGroupMeta acquires lock on group meta data
func (b *Backend) lockGroupMeta(groupMeta *tasks.GroupMeta) error {
	groupMeta.Lock = true
	encoded, err := json.Marshal(groupMeta)
	if err != nil {
		return err
	}

	return b.getClient().Set(&gomemcache.Item{
		Key:        groupMeta.GroupUUID,
		Value:      encoded,
		Expiration: b.getExpirationTimestamp(),
	})
}

// unlockGroupMeta releases lock on group meta data
func (b *Backend) unlockGroupMeta(groupMeta *tasks.GroupMeta) error {
	groupMeta.Lock = false
	encoded, err := json.Marshal(groupMeta)
	if err != nil {
		return err
	}

	return b.getClient().Set(&gomemcache.Item{
		Key:        groupMeta.GroupUUID,
		Value:      encoded,
		Expiration: b.getExpirationTimestamp(),
	})
}

// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *Backend) getGroupMeta(groupUUID string) (*tasks.GroupMeta, error) {
	item, err := b.getClient().Get(groupUUID)
	if err != nil {
		return nil, err
	}

	groupMeta := new(tasks.GroupMeta)
	decoder := json.NewDecoder(bytes.NewReader(item.Value))
	decoder.UseNumber()
	if err := decoder.Decode(groupMeta); err != nil {
		return nil, err
	}

	return groupMeta, nil
}

// getStates returns multiple task states
func (b *Backend) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	states := make([]*tasks.TaskState, len(taskUUIDs))

	for i, taskUUID := range taskUUIDs {
		item, err := b.getClient().Get(taskUUID)
		if err != nil {
			return nil, err
		}

		state := new(tasks.TaskState)
		decoder := json.NewDecoder(bytes.NewReader(item.Value))
		decoder.UseNumber()
		if err := decoder.Decode(state); err != nil {
			return nil, err
		}

		states[i] = state
	}

	return states, nil
}

// getExpirationTimestamp returns expiration timestamp
func (b *Backend) getExpirationTimestamp() int32 {
	expiresIn := b.GetConfig().ResultsExpireIn
	if expiresIn == 0 {
		// // expire results after 1 hour by default
		expiresIn = config.DefaultResultsExpireIn
	}
	return int32(time.Now().Unix() + int64(expiresIn))
}

// getClient returns or creates instance of Memcache client
func (b *Backend) getClient() *gomemcache.Client {
	if b.client == nil {
		b.client = gomemcache.New(b.servers...)
	}
	return b.client
}
