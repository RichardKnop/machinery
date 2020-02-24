package cassandra

import (
	"bytes"
	"encoding/json"

	"github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/gocql/gocql"
)

// Backend represents a Redis result backend
type Backend struct {
	common.Backend
	sess *gocql.Session
}

// New returns a new cassandra backend.
func New(cnf *config.Config) (iface.Backend, error) {
	cluster := gocql.NewCluster(cnf.Cassandra.Hosts...)
	cluster.Keyspace = cnf.Cassandra.Keyspace
	cluster.Consistency = gocql.Quorum
	if cnf.Cassandra.Username != "" && cnf.Cassandra.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cnf.Cassandra.Username,
			Password: cnf.Cassandra.Password,
		}
	}
	sess, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &Backend{
		Backend: common.NewBackend(cnf),
		sess:    sess,
	}, nil
}

// SetStatePending updates task state to PENDING.
func (b *Backend) SetStatePending(signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	return b.updateState(taskState)
}

// SetStateReceived updates task state to RECEIVED.
func (b *Backend) SetStateReceived(signature *tasks.Signature) error {
	taskState := tasks.NewReceivedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateStarted updates task state to STARTED.
func (b *Backend) SetStateStarted(signature *tasks.Signature) error {
	taskState := tasks.NewStartedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateRetry updates task state to RETRY.
func (b *Backend) SetStateRetry(signature *tasks.Signature) error {
	state := tasks.NewRetryTaskState(signature)
	return b.updateState(state)
}

// SetStateSuccess updates task state to SUCCESS.
func (b *Backend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	taskState := tasks.NewSuccessTaskState(signature, results)
	return b.updateState(taskState)
}

// SetStateFailure updates task state to FAILURE.
func (b *Backend) SetStateFailure(signature *tasks.Signature, err string) error {
	taskState := tasks.NewFailureTaskState(signature, err)
	return b.updateState(taskState)
}

// GetState returns the latest task state.
func (b *Backend) GetState(taskUUID string) (*tasks.TaskState, error) {
	var data []byte
	if err := b.sess.Query(`SELECT data FROM job WHERE id = ?`,
		taskUUID).Scan(&data); err != nil {
		return nil, err
	}
	state := tasks.TaskState{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&state); err != nil {
		return nil, err
	}
	return &state, nil
}

// PurgeState deletes stored task state.
func (b *Backend) PurgeState(taskUUID string) error {
	return b.sess.Query(`DELETE FROM job WHERE id = ?`, taskUUID).Exec()
}

// PurgeGroupMeta deletes stored group meta data.
func (b *Backend) PurgeGroupMeta(groupUUID string) error {
	return nil
}

// IsAMQP returns if backend uses AMQP protocol or not.
func (b *Backend) IsAMQP() bool {
	return false
}

// InitGroup initializes a group task. TODO
func (b *Backend) InitGroup(groupUUID string, taskUUIDs []string) error {
	return nil
}

// GroupCompleted checks if all group tasks are completed. TODO
func (b *Backend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	return true, nil
}

// GroupTaskStates returnes state of individual group tasks.
func (b *Backend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	return []*tasks.TaskState{}, nil
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false). TODO
func (b *Backend) TriggerChord(groupUUID string) (bool, error) {
	return true, nil
}

// updateState saves current task state.
func (b *Backend) updateState(taskState *tasks.TaskState) error {
	encoded, err := json.Marshal(taskState)
	if err != nil {
		return err
	}
	return b.sess.Query(`INSERT INTO job (id, name, data, state) VALUES (?, ?, ?, ?) USING TTL ?`,
		taskState.TaskUUID, taskState.TaskName, encoded, taskState.State, b.ttl()).Exec()
}

// ttl returns TTL in seconds.
func (b *Backend) ttl() int {
	expiresIn := b.GetConfig().ResultsExpireIn
	if expiresIn == 0 {
		expiresIn = config.DefaultResultsExpireIn
	}
	return expiresIn
}
