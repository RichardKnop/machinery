package backends

import (
	"encoding/json"

	"github.com/bradfitz/gomemcache/memcache"
)

// MemcacheBackend represents a Memcache result backend
type MemcacheBackend struct {
	client *memcache.Client
}

// NewMemcacheBackend creates MemcacheBackend instance
func NewMemcacheBackend(servers []string) Backend {
	return Backend(&MemcacheBackend{
		client: memcache.New(servers...),
	})
}

// UpdateState updates a task state
func (memcacheBackend *MemcacheBackend) UpdateState(taskState *TaskState) error {
	encoded, err := json.Marshal(&taskState)
	if err != nil {
		return err
	}

	return memcacheBackend.client.Set(&memcache.Item{
		Key:   taskState.TaskUUID,
		Value: encoded,
	})
}

// GetState returns the latest task state
func (memcacheBackend *MemcacheBackend) GetState(taskUUID string) (*TaskState, error) {
	taskState := TaskState{}

	item, err := memcacheBackend.client.Get(taskUUID)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(item.Value, taskState); err != nil {
		return nil, err
	}

	return &taskState, nil
}
