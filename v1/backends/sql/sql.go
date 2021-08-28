package sql

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"gorm.io/gorm"
)

type Backend struct {
	common.Backend
	db *gorm.DB
}

// New creates Backend instance
func New(cnf *config.Config) iface.Backend {
	if err := cnf.SQL.Client.AutoMigrate(&GroupMetadata{}, &TaskState{}); err != nil {
		panic(err)
	}
	return &Backend{
		Backend: common.NewBackend(cnf),
		db:      cnf.SQL.Client,
	}
}

// InitGroup creates and saves a group meta data object
func (b *Backend) InitGroup(groupUUID string, taskUUIDs []string) error {
	groupMeta := &GroupMetadata{
		GroupUUID: groupUUID,
		TaskUUIDS: strings.Join(taskUUIDs, taskUUIDSplitSep),
		CreatedAt: time.Now().UTC(),
	}

	return b.db.Create(groupMeta).Error
}

// GroupCompleted returns true if all tasks in a group finished
func (b *Backend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	mGroup, err := b.getGroup(groupUUID)
	if err != nil {
		return false, err
	}
	var c int64 = 0
	taskUUIDS := strings.Split(mGroup.TaskUUIDS, taskUUIDSplitSep)
	if err := b.db.Table(taskStateTableName).Where("uuid in ? AND state in ?", taskUUIDS, tasks.StateIsCompleted).
		Count(&c).Error; err != nil {
		return false, err
	}
	return int64(groupTaskCount) == c, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *Backend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	mGroup, err := b.getGroup(groupUUID)
	if err != nil {
		return nil, err
	}
	mTasks := make([]*TaskState, 0)
	taskUUIDS := strings.Split(mGroup.TaskUUIDS, taskUUIDSplitSep)
	if err := b.db.Table(taskStateTableName).Where("uuid in ?", taskUUIDS).Find(&mTasks).Error; err != nil {
		return nil, err
	}
	taskStates, err := b.mTasks2TaskStates(mTasks...)
	if err != nil {
		return nil, err
	}
	return taskStates, nil
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never triggerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *Backend) TriggerChord(groupUUID string) (bool, error) {
	result := b.db.Table(groupMetadataTableName).
		Where("group_uuid = ? AND chord_triggered = false", groupUUID).
		Update("chord_triggered", true)
	if result.Error != nil {
		return false, result.Error
	}
	if result.RowsAffected == 0 {
		return false, nil
	}
	return true, nil
}

// SetStatePending updates task state to PENDING
func (b *Backend) SetStatePending(signature *tasks.Signature) error {
	m := &TaskState{
		UUID:      signature.UUID,
		Name:      signature.Name,
		State:     tasks.StatePending,
		CreatedAt: time.Now().UTC(),
	}
	return b.db.Create(m).Error
}

// SetStateReceived updates task state to RECEIVED
func (b *Backend) SetStateReceived(signature *tasks.Signature) error {
	return b.updateState(signature.UUID, tasks.StateReceived)
}

// SetStateStarted updates task state to STARTED
func (b *Backend) SetStateStarted(signature *tasks.Signature) error {
	return b.updateState(signature.UUID, tasks.StateStarted)
}

// SetStateRetry updates task state to RETRY
func (b *Backend) SetStateRetry(signature *tasks.Signature) error {
	return b.updateState(signature.UUID, tasks.StateRetry)
}

// SetStateSuccess updates task state to SUCCESS
func (b *Backend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	encoded, err := json.Marshal(results)
	if err != nil {
		return err
	}
	return b.db.Table(taskStateTableName).Where("uuid = ?", signature.UUID).
		Updates(map[string]interface{}{"state": tasks.StateSuccess, "results": string(encoded)}).Error
}

// SetStateFailure updates task state to FAILURE
func (b *Backend) SetStateFailure(signature *tasks.Signature, err string) error {
	return b.db.Table(taskStateTableName).Where("uuid = ?", signature.UUID).
		Updates(map[string]interface{}{"state": tasks.StateFailure, "error": err}).Error
}

// GetState returns the latest task state
func (b *Backend) GetState(taskUUID string) (*tasks.TaskState, error) {
	m := &TaskState{}
	if err := b.db.Where("uuid = ?", taskUUID).First(m).Error; err != nil {
		return nil, err
	}
	taskStates, err := b.mTasks2TaskStates(m)
	if err != nil {
		return nil, err
	}
	return taskStates[0], nil
}

// PurgeState deletes stored task state
func (b *Backend) PurgeState(taskUUID string) error {
	return b.db.Where("uuid = ?", taskUUID).Delete(&TaskState{}).Error

}

// PurgeGroupMeta deletes stored group meta data
func (b *Backend) PurgeGroupMeta(groupUUID string) error {
	return b.db.Where("group_uuid = ?", groupUUID).Delete(&GroupMetadata{}).Error
}

func (b *Backend) updateState(uuid string, state string) error {
	return b.db.Table(taskStateTableName).Where("uuid = ?", uuid).
		Updates(map[string]interface{}{"state": state}).Error
}

func (b *Backend) getGroup(groupUUID string) (*GroupMetadata, error) {
	mGroup := &GroupMetadata{}
	if err := b.db.Where("group_uuid = ?", groupUUID).First(mGroup).Error; err != nil {
		return nil, err
	}
	return mGroup, nil
}

func (b *Backend) mTasks2TaskStates(mTasks ...*TaskState) ([]*tasks.TaskState, error) {
	takeStates := make([]*tasks.TaskState, len(mTasks), len(mTasks))
	for i, m := range mTasks {
		results := make([]*tasks.TaskResult, 0)
		if m.Results != "" {
			if err := json.Unmarshal([]byte(m.Results), &results); err != nil {
				return nil, err
			}
		}
		takeStates[i] = &tasks.TaskState{
			TaskUUID:  m.UUID,
			TaskName:  m.Name,
			State:     m.State,
			Results:   results,
			Error:     m.Error,
			CreatedAt: m.CreatedAt,
		}
	}
	return takeStates, nil
}
