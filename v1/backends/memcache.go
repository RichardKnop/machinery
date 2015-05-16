package backends

import (
	"encoding/json"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/bradfitz/gomemcache/memcache"
)

// MemcacheBackend represents a Memcache result backend
type MemcacheBackend struct {
	config *config.Config
	client *memcache.Client
}

// NewMemcacheBackend creates MemcacheBackend instance
func NewMemcacheBackend(cnf *config.Config, servers []string) Backend {
	return Backend(&MemcacheBackend{
		config: cnf,
		client: memcache.New(servers...),
	})
}

// UpdateState updates a task state
func (memcacheBackend *MemcacheBackend) UpdateState(taskState *TaskState) error {
	encoded, err := json.Marshal(&taskState)
	if err != nil {
		return err
	}

	expiration := memcacheBackend.config.ResultsExpireIn / 1000
	if expiration == 0 {
		// // expire results after 1 hour by default
		expiration = 3600
	}

	return memcacheBackend.client.Set(&memcache.Item{
		Key:        taskState.TaskUUID,
		Value:      encoded,
		Expiration: int32(expiration),
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
