package backends

import (
	"encoding/json"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
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

// UpdateState updates a task state
func (memcacheBackend *MemcacheBackend) UpdateState(taskState *TaskState) error {
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

	//time.Sleep(1 * time.Millisecond)

	return nil
}

// GetState returns the latest task state
func (memcacheBackend *MemcacheBackend) GetState(taskUUID string) (*TaskState, error) {
	taskState := TaskState{}

	client := memcache.New(memcacheBackend.servers...)

	item, err := client.Get(taskUUID)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(item.Value, &taskState); err != nil {
		return nil, err
	}

	return &taskState, nil
}
