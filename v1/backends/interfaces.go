package backends

import (
	"github.com/RichardKnop/machinery/v1/signatures"
)

// Backend - a common interface for all result backends
type Backend interface {
	SetStatePending(signature *signatures.TaskSignature) error
	SetStateReceived(signature *signatures.TaskSignature) error
	SetStateStarted(signature *signatures.TaskSignature) error
	SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) error
	SetStateFailure(signature *signatures.TaskSignature, err string) error
	GetState(signature *signatures.TaskSignature) (*TaskState, error)
	PurgeState(signature *signatures.TaskSignature) error
}
