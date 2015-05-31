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
	return memcacheBackend.updateState(NewPendingTaskState(signature))
}

// SetStateReceived - sets task state to RECEIVED
func (memcacheBackend *MemcacheBackend) SetStateReceived(signature *signatures.TaskSignature) error {
	return memcacheBackend.updateState(NewReceivedTaskState(signature))
}

// SetStateStarted - sets task state to STARTED
func (memcacheBackend *MemcacheBackend) SetStateStarted(signature *signatures.TaskSignature) error {
	return memcacheBackend.updateState(NewStartedTaskState(signature))
}

// SetStateSuccess - sets task state to SUCCESS
func (memcacheBackend *MemcacheBackend) SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) error {
	return memcacheBackend.updateState(NewSuccessTaskState(signature, result))
}

// SetStateFailure - sets task state to FAILURE
func (memcacheBackend *MemcacheBackend) SetStateFailure(signature *signatures.TaskSignature, err string) error {
	return memcacheBackend.updateState(NewFailureTaskState(signature, err))
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

	expiresIn := memcacheBackend.config.ResultsExpireIn
	if expiresIn == 0 {
		// // expire results after 1 hour by default
		expiresIn = 3600
	}
	expirationTimestamp := int32(time.Now().Unix() + int64(expiresIn))

	if err := client.Touch(
		taskState.TaskUUID,
		expirationTimestamp,
	); err != nil {
		return err
	}

	return nil
}
