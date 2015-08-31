package backends

import (
	"github.com/RichardKnop/machinery/v1/signatures"
)

// Backend - a common interface for all result backends
type Backend interface {
	SetStatePending(signature *signatures.TaskSignature) error
	SetStateReceived(signature *signatures.TaskSignature) error
	SetStateStarted(signature *signatures.TaskSignature) error
	SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) (*TaskStateGroup, error)
	SetStateFailure(signature *signatures.TaskSignature, err string) (*TaskStateGroup, error)
	GetState(taskUUID string) (*TaskState, error)
	PurgeState(taskState *TaskState) error
	PurgeStateGroup(taskStateGroup *TaskStateGroup) error
}
