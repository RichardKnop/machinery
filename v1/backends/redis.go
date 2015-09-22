package backends

import (
	"encoding/json"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
)

// RedisBackend represents a Memcache result backend
type RedisBackend struct {
	config *config.Config
	host   string
	conn   redis.Conn
}

// NewRedisBackend creates RedisBackend instance
func NewRedisBackend(cnf *config.Config, host string) Backend {
	return Backend(&RedisBackend{
		config: cnf,
		host:   host,
	})
}

// InitGroup - saves UUIDs of all tasks in a group
func (redisBackend *RedisBackend) InitGroup(groupUUID string, taskUUIDs []string) error {
	groupMeta := &GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
	}

	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return err
	}

	conn, err := redisBackend.open()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("SET", groupUUID, encoded)
	if err != nil {
		return err
	}

	return redisBackend.setExpirationTime(groupUUID)
}

// GroupCompleted - returns true if all tasks in a group finished
func (redisBackend *RedisBackend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta := GroupMeta{}

	conn, err := redisBackend.open()
	if err != nil {
		return false, err
	}
	defer conn.Close()

	item, err := redis.Bytes(conn.Do("GET", groupUUID))
	if err != nil {
		return false, err
	}

	if err := json.Unmarshal(item, &groupMeta); err != nil {
		return false, err
	}

	for _, taskUUID := range groupMeta.TaskUUIDs {
		taskState, err := redisBackend.GetState(taskUUID)
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
func (redisBackend *RedisBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*TaskState, error) {
	taskStates := make([]*TaskState, groupTaskCount)

	groupMeta := GroupMeta{}

	conn, err := redisBackend.open()
	if err != nil {
		return taskStates, err
	}
	defer conn.Close()

	item, err := redis.Bytes(conn.Do("GET", groupUUID))
	if err != nil {
		return taskStates, err
	}

	if err := json.Unmarshal(item, &groupMeta); err != nil {
		return taskStates, err
	}

	for i, taskUUID := range groupMeta.TaskUUIDs {
		taskState, err := redisBackend.GetState(taskUUID)
		if err != nil {
			return taskStates, err
		}

		taskStates[i] = taskState
	}

	return taskStates, nil
}

// SetStatePending - sets task state to PENDING
func (redisBackend *RedisBackend) SetStatePending(signature *signatures.TaskSignature) error {
	taskState := NewPendingTaskState(signature)
	return redisBackend.updateState(taskState)
}

// SetStateReceived - sets task state to RECEIVED
func (redisBackend *RedisBackend) SetStateReceived(signature *signatures.TaskSignature) error {
	taskState := NewReceivedTaskState(signature)
	return redisBackend.updateState(taskState)
}

// SetStateStarted - sets task state to STARTED
func (redisBackend *RedisBackend) SetStateStarted(signature *signatures.TaskSignature) error {
	taskState := NewStartedTaskState(signature)
	return redisBackend.updateState(taskState)
}

// SetStateSuccess - sets task state to SUCCESS
func (redisBackend *RedisBackend) SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) error {
	taskState := NewSuccessTaskState(signature, result)
	return redisBackend.updateState(taskState)
}

// SetStateFailure - sets task state to FAILURE
func (redisBackend *RedisBackend) SetStateFailure(signature *signatures.TaskSignature, err string) error {
	taskState := NewFailureTaskState(signature, err)
	return redisBackend.updateState(taskState)
}

// GetState - returns the latest task state
func (redisBackend *RedisBackend) GetState(taskUUID string) (*TaskState, error) {
	taskState := TaskState{}

	conn, err := redisBackend.open()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	item, err := redis.Bytes(conn.Do("GET", taskUUID))
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(item, &taskState); err != nil {
		return nil, err
	}

	return &taskState, nil
}

// PurgeState - deletes stored task state
func (redisBackend *RedisBackend) PurgeState(taskUUID string) error {
	conn, err := redisBackend.open()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("DEL", taskUUID)
	if err != nil {
		return err
	}

	return nil
}

// PurgeGroupMeta - deletes stored group meta data
func (redisBackend *RedisBackend) PurgeGroupMeta(groupUUID string) error {
	conn, err := redisBackend.open()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("DEL", groupUUID)
	if err != nil {
		return err
	}

	return nil
}

// Updates a task state
func (redisBackend *RedisBackend) updateState(taskState *TaskState) error {
	encoded, err := json.Marshal(&taskState)
	if err != nil {
		return err
	}

	conn, err := redisBackend.open()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("SET", taskState.TaskUUID, encoded)
	if err != nil {
		return err
	}

	return redisBackend.setExpirationTime(taskState.TaskUUID)
}

// Sets expiration timestamp on a stored state
func (redisBackend *RedisBackend) setExpirationTime(key string) error {
	expiresIn := redisBackend.config.ResultsExpireIn
	if expiresIn == 0 {
		// // expire results after 1 hour by default
		expiresIn = 3600
	}
	expirationTimestamp := int32(time.Now().Unix() + int64(expiresIn))

	conn, err := redisBackend.open()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("EXPIREAT", key, expirationTimestamp)
	if err != nil {
		return err
	}

	return nil
}

// Returns / creates instance of Redis connection
func (redisBackend *RedisBackend) open() (redis.Conn, error) {
	return redis.Dial("tcp", redisBackend.host)
}
