package backends

import (
	"strings"
	"time"
)

type mongodbTaskState struct {
	TaskUUID   string               `bson:"_id"`
	CreateTime time.Time            `bson:"createdAt"`
	State      string               `bson:"state"`
	Results    []*mongodbTaskResult `bson:"results"`
	Error      string               `bson:"error"`
}

func (t *mongodbTaskState) TaskState() *TaskState {
	taskState := &TaskState{
		TaskUUID: t.TaskUUID,
		State:    t.State,
	}

	if taskState.State == SuccessState {
		taskState.Results = make([]*TaskResult, len(t.Results))
		for i, result := range t.Results {
			taskState.Results[i] = result.TaskResult()
		}
	} else if taskState.State == FailureState {
		taskState.Error = t.Error
	}
	return taskState
}

type mongodbGroupMeta struct {
	GroupUUID      string   `bson:"_id"`
	TaskUUIDs      []string `bson:"task_uuids"`
	ChordTriggered bool     `bson:"chord_trigerred"`
}

func (t *mongodbGroupMeta) GroupMeta() *GroupMeta {
	groupMeta := &GroupMeta{
		GroupUUID:      t.GroupUUID,
		TaskUUIDs:      t.TaskUUIDs,
		ChordTriggered: t.ChordTriggered,
	}

	return groupMeta
}

type mongodbTaskResult struct {
	Type  string      `bson:"type"`
	Value interface{} `bson:"value"`
}

func (r *mongodbTaskResult) TaskResult() *TaskResult {
	var value interface{}
	value = r.Value

	if strings.HasPrefix(r.Type, "int") {
		value = float64(value.(int64))
	} else if strings.HasPrefix(r.Type, "uint") {
		value = float64(value.(uint64))
	} else if strings.HasPrefix(r.Type, "float") {
		value = float64(value.(float64))
	}

	return &TaskResult{
		Type:  r.Type,
		Value: value,
	}
}
