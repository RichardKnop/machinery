package blank

import (
	"github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// Backend represents a "blank" result backend
type Backend struct {
	common.Backend
}

// New creates blank Backend instance
func New() iface.Backend {
	return &Backend{
		Backend: common.NewBackend(new(config.Config)),
	}
}

// InitGroup creates and saves a group meta data object
func (b *Backend) InitGroup(groupUUID string, taskUUIDs []string) error {
	return nil
}

// GroupCompleted returns true if all tasks in a group finished
func (b *Backend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	return false, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *Backend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	ret := make([]*tasks.TaskState, 0, groupTaskCount)
	return ret, nil
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *Backend) TriggerChord(groupUUID string) (bool, error) {
	return true, nil
}

// SetStatePending updates task state to PENDING
func (b *Backend) SetStatePending(signature *tasks.Signature) error {
	return nil
}

// SetStateReceived updates task state to RECEIVED
func (b *Backend) SetStateReceived(signature *tasks.Signature) error {
	return nil
}

// SetStateStarted updates task state to STARTED
func (b *Backend) SetStateStarted(signature *tasks.Signature) error {
	return nil
}

// SetStateRetry updates task state to RETRY
func (b *Backend) SetStateRetry(signature *tasks.Signature) error {
	return nil
}

// SetStateSuccess updates task state to SUCCESS
func (b *Backend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	return nil
}

// SetStateFailure updates task state to FAILURE
func (b *Backend) SetStateFailure(signature *tasks.Signature, err string) error {
	return nil
}

// GetState returns the latest task state
func (b *Backend) GetState(taskUUID string) (*tasks.TaskState, error) {
	return nil, nil
}

// PurgeState deletes stored task state
func (b *Backend) PurgeState(taskUUID string) error {
	return nil
}

// PurgeGroupMeta deletes stored group meta data
func (b *Backend) PurgeGroupMeta(groupUUID string) error {
	return nil
}

func (b *Backend) IsAMQP() bool {
	return false
}
