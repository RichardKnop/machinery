package backends

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/RichardKnop/machinery/Godeps/_workspace/src/github.com/garyburd/redigo/redis"
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

// SetStatePending - sets task state to PENDING
func (redisBackend *RedisBackend) SetStatePending(signature *signatures.TaskSignature) error {
	taskState := NewPendingTaskState(signature)

	if err := redisBackend.updateState(taskState); err != nil {
		return err
	}

	if signature.GroupUUID == "" {
		return nil
	}

	_, err := redisBackend.updateStateGroup(
		signature.GroupUUID,
		signature.GroupTaskCount,
		signature.UUID,
	)
	return err
}

// SetStateReceived - sets task state to RECEIVED
func (redisBackend *RedisBackend) SetStateReceived(signature *signatures.TaskSignature) error {
	taskState := NewReceivedTaskState(signature)

	if err := redisBackend.updateState(taskState); err != nil {
		return err
	}

	if signature.GroupUUID == "" {
		return nil
	}

	_, err := redisBackend.updateStateGroup(
		signature.GroupUUID,
		signature.GroupTaskCount,
		signature.UUID,
	)
	return err
}

// SetStateStarted - sets task state to STARTED
func (redisBackend *RedisBackend) SetStateStarted(signature *signatures.TaskSignature) error {
	taskState := NewStartedTaskState(signature)

	if err := redisBackend.updateState(taskState); err != nil {
		return err
	}

	if signature.GroupUUID == "" {
		return nil
	}

	_, err := redisBackend.updateStateGroup(
		signature.GroupUUID,
		signature.GroupTaskCount,
		signature.UUID,
	)
	return err
}

// SetStateSuccess - sets task state to SUCCESS
func (redisBackend *RedisBackend) SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) (*TaskStateGroup, error) {
	taskState := NewSuccessTaskState(signature, result)

	if err := redisBackend.updateState(taskState); err != nil {
		return nil, err
	}

	if signature.GroupUUID == "" {
		return nil, nil
	}

	return redisBackend.updateStateGroup(
		signature.GroupUUID,
		signature.GroupTaskCount,
		signature.UUID,
	)
}

// SetStateFailure - sets task state to FAILURE
func (redisBackend *RedisBackend) SetStateFailure(signature *signatures.TaskSignature, err string) error {
	taskState := NewFailureTaskState(signature, err)

	if err := redisBackend.updateState(taskState); err != nil {
		return err
	}

	if signature.GroupUUID == "" {
		return nil
	}

	_, errr := redisBackend.updateStateGroup(
		signature.GroupUUID,
		signature.GroupTaskCount,
		signature.UUID,
	)
	return errr
}

// GetState returns the latest task state
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
func (redisBackend *RedisBackend) PurgeState(taskState *TaskState) error {
	conn, err := redisBackend.open()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("DEL", taskState.TaskUUID)
	if err != nil {
		return err
	}

	return nil
}

// PurgeStateGroup - deletes stored task state
func (redisBackend *RedisBackend) PurgeStateGroup(taskStateGroup *TaskStateGroup) error {
	conn, err := redisBackend.open()
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Do("DEL", taskStateGroup.GroupUUID)
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

// Updates a task state group
func (redisBackend *RedisBackend) updateStateGroup(groupUUID string, groupTaskCount int, taskUUID string) (*TaskStateGroup, error) {
	if groupUUID == "" || groupTaskCount == 0 {
		return nil, nil
	}

	conn, err := redisBackend.open()
	if err != nil {
		return nil, err
	}

	defer conn.Close()

	var taskStateGroup *TaskStateGroup

	item, err := redis.Bytes(conn.Do("GET", groupUUID))

	if err != nil {
		taskStateGroup = &TaskStateGroup{
			GroupUUID:      groupUUID,
			GroupTaskCount: groupTaskCount,
			States:         make(map[string]*TaskState),
		}
	} else {
		if err := json.Unmarshal(item, &taskStateGroup); err != nil {
			return nil, err
		}
	}

	taskState, err := redisBackend.GetState(taskUUID)
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
		taskState, err := redisBackend.GetState(uuid)
		if err != nil {
			return nil, err
		}
		taskStateGroup.States[uuid] = taskState
	}

	encoded, err := json.Marshal(taskStateGroup)
	if err != nil {
		return nil, fmt.Errorf("JSON Encode Message: %v", err)
	}

	_, err = conn.Do("SET", groupUUID, encoded)
	if err != nil {
		return nil, err
	}

	return taskStateGroup, redisBackend.setExpirationTime(groupUUID)
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
