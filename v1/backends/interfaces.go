package backends

import (
	"github.com/RichardKnop/machinery/v1/signatures"
)

// Backend - a common interface for all result backends
type Backend interface {
	// Group related functions
	InitGroup(groupUUID string, taskUUIDs []string) error
	GroupCompleted(groupUUID string, groupTaskCount int) (bool, error)
	GroupTaskStates(groupUUID string, groupTaskCount int) ([]*TaskState, error)
	// Setting / getting task state
	SetStatePending(signature *signatures.TaskSignature) error
	SetStateReceived(signature *signatures.TaskSignature) error
	SetStateStarted(signature *signatures.TaskSignature) error
	SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) error
	SetStateFailure(signature *signatures.TaskSignature, err string) error
	GetState(taskUUID string) (*TaskState, error)
	// Purging stored stored tasks states and group meta data
	PurgeState(taskUUID string) error
	PurgeGroupMeta(groupUUID string) error
}
