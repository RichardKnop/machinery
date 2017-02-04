package backends

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/logger"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/garyburd/redigo/redis"
	"gopkg.in/redsync.v1"
)

// RedisBackend represents a Memcache result backend
type RedisBackend struct {
	config   *config.Config
	host     string
	password string
	db       int
	pool     *redis.Pool
	// If set, path to a socket file overrides hostname
	socketPath string
	redsync    *redsync.Redsync
}

// NewRedisBackend creates RedisBackend instance
func NewRedisBackend(cnf *config.Config, host, password, socketPath string, db int) Backend {
	return Backend(&RedisBackend{
		config:     cnf,
		host:       host,
		db:         db,
		password:   password,
		socketPath: socketPath,
	})
}

// InitGroup - saves UUIDs of all tasks in a group
func (b *RedisBackend) InitGroup(groupUUID string, taskUUIDs []string) error {
	groupMeta := &GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
	}

	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return err
	}

	conn := b.open()
	defer conn.Close()

	_, err = conn.Do("SET", groupUUID, encoded)
	if err != nil {
		return err
	}

	return b.setExpirationTime(groupUUID)
}

// GroupCompleted - returns true if all tasks in a group finished
func (b *RedisBackend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}

	for _, taskState := range taskStates {
		if !taskState.IsCompleted() {
			return false, nil
		}
	}

	return true, nil
}

// GroupTaskStates - returns states of all tasks in the group
func (b *RedisBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*TaskState, error) {
	taskStates := make([]*TaskState, groupTaskCount)

	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return taskStates, err
	}

	return b.getStates(groupMeta.TaskUUIDs...)
}

// TriggerChord - marks chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *RedisBackend) TriggerChord(groupUUID string) (bool, error) {
	conn := b.open()
	defer conn.Close()

	m := b.redsync.NewMutex("TriggerChordMutex")
	if err := m.Lock(); err != nil {
		return false, err
	}
	defer m.Unlock()

	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	// If the chord has been triggered already, return false and vice-versa
	shouldTrigger := !groupMeta.ChordTriggered
	if !groupMeta.ChordTriggered {
		// Set flag to true
		groupMeta.ChordTriggered = true
	}

	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return false, err
	}

	_, err = conn.Do("SET", groupUUID, encoded)
	if err != nil {
		return false, err
	}

	return shouldTrigger, nil
}

// SetStatePending - sets task state to PENDING
func (b *RedisBackend) SetStatePending(signature *signatures.TaskSignature) error {
	taskState := NewPendingTaskState(signature)
	return b.updateState(taskState)
}

// SetStateReceived - sets task state to RECEIVED
func (b *RedisBackend) SetStateReceived(signature *signatures.TaskSignature) error {
	taskState := NewReceivedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateStarted - sets task state to STARTED
func (b *RedisBackend) SetStateStarted(signature *signatures.TaskSignature) error {
	taskState := NewStartedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateSuccess - sets task state to SUCCESS
func (b *RedisBackend) SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) error {
	taskState := NewSuccessTaskState(signature, result)
	return b.updateState(taskState)
}

// SetStateFailure - sets task state to FAILURE
func (b *RedisBackend) SetStateFailure(signature *signatures.TaskSignature, err string) error {
	taskState := NewFailureTaskState(signature, err)
	return b.updateState(taskState)
}

// GetState - returns the latest task state
func (b *RedisBackend) GetState(taskUUID string) (*TaskState, error) {
	taskState := new(TaskState)

	conn := b.open()
	defer conn.Close()

	item, err := redis.Bytes(conn.Do("GET", taskUUID))
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(item, taskState); err != nil {
		return nil, err
	}

	return taskState, nil
}

// PurgeState - deletes stored task state
func (b *RedisBackend) PurgeState(taskUUID string) error {
	conn := b.open()
	defer conn.Close()

	_, err := conn.Do("DEL", taskUUID)
	if err != nil {
		return err
	}

	return nil
}

// PurgeGroupMeta - deletes stored group meta data
func (b *RedisBackend) PurgeGroupMeta(groupUUID string) error {
	conn := b.open()
	defer conn.Close()

	_, err := conn.Do("DEL", groupUUID)
	if err != nil {
		return err
	}

	return nil
}

// Fetches GroupMeta from the backend, convenience function to avoid repetition
func (b *RedisBackend) getGroupMeta(groupUUID string) (*GroupMeta, error) {
	conn := b.open()
	defer conn.Close()

	item, err := redis.Bytes(conn.Do("GET", groupUUID))
	if err != nil {
		return nil, err
	}

	groupMeta := new(GroupMeta)
	if err := json.Unmarshal(item, &groupMeta); err != nil {
		return nil, err
	}

	return groupMeta, nil
}

// getStates Returns multiple task states with MGET
func (b *RedisBackend) getStates(taskUUIDs ...string) ([]*TaskState, error) {
	taskStates := make([]*TaskState, len(taskUUIDs))

	conn := b.open()
	defer conn.Close()

	// conn.Do requires []interface{}... can't pass []string unfortunately
	taskUUIDInterfaces := make([]interface{}, len(taskUUIDs))
	for i, taskUUID := range taskUUIDs {
		taskUUIDInterfaces[i] = interface{}(taskUUID)
	}

	reply, err := redis.Values(conn.Do("MGET", taskUUIDInterfaces...))
	if err != nil {
		return taskStates, err
	}

	for i, value := range reply {
		bytes, ok := value.([]byte)
		if !ok {
			return taskStates, fmt.Errorf("Expected byte array, instead got: %v", value)
		}

		taskState := new(TaskState)
		if err := json.Unmarshal(bytes, taskState); err != nil {
			logger.Get().Print(err)
			return taskStates, err
		}

		taskStates[i] = taskState
	}

	return taskStates, nil
}

// Updates a task state
func (b *RedisBackend) updateState(taskState *TaskState) error {
	conn := b.open()
	defer conn.Close()

	encoded, err := json.Marshal(&taskState)
	if err != nil {
		return err
	}

	_, err = conn.Do("SET", taskState.TaskUUID, encoded)
	if err != nil {
		return err
	}

	return b.setExpirationTime(taskState.TaskUUID)
}

// Sets expiration timestamp on a stored state
func (b *RedisBackend) setExpirationTime(key string) error {
	expiresIn := b.config.ResultsExpireIn
	if expiresIn == 0 {
		// // expire results after 1 hour by default
		expiresIn = 3600
	}
	expirationTimestamp := int32(time.Now().Unix() + int64(expiresIn))

	conn := b.open()
	defer conn.Close()

	_, err := conn.Do("EXPIREAT", key, expirationTimestamp)
	if err != nil {
		return err
	}

	return nil
}

// Returns / creates instance of Redis connection
func (b *RedisBackend) open() redis.Conn {
	if b.pool == nil {
		b.pool = b.newPool()
	}
	if b.redsync == nil {
		var pools = []redsync.Pool{b.pool}
		b.redsync = redsync.New(pools)
	}
	return b.pool.Get()
}

// Returns a new pool of Redis connections
func (b *RedisBackend) newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			var (
				c    redis.Conn
				err  error
				opts = make([]redis.DialOption, 0)
			)

			if b.password != "" {
				opts = append(opts, redis.DialPassword(b.password))
			}

			if b.socketPath != "" {
				c, err = redis.Dial("unix", b.socketPath, opts...)
			} else {
				c, err = redis.Dial("tcp", b.host, opts...)
			}

			if b.db != 0 {
				_, err = c.Do("SELECT", b.db)
			}

			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
