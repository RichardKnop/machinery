package iface

import (
	"errors"
	"fmt"
	"strings"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// Backend - a common interface for all result backends
type Backend interface {
	// Group related functions
	InitGroup(groupUUID string, taskUUIDs []string) error
	GroupCompleted(groupUUID string, groupTaskCount int) (bool, error)
	GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error)
	TriggerChord(groupUUID string) (bool, error)

	// Setting / getting task state
	SetStatePending(signature *tasks.Signature) error
	SetStateReceived(signature *tasks.Signature) error
	SetStateStarted(signature *tasks.Signature) error
	SetStateRetry(signature *tasks.Signature) error
	SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error
	SetStateFailure(signature *tasks.Signature, err string) error
	GetState(taskUUID string) (*tasks.TaskState, error)

	// Purging stored stored tasks states and group meta data
	IsAMQP() bool
	PurgeState(taskUUID string) error
	PurgeGroupMeta(groupUUID string) error
}

var BackendFactories = map[string]func(*config.Config) (Backend, error){}

// BackendFactory creates a new object of backends.Interface
// Currently supported backends are AMQP/S and Memcache
func BackendFactory(cnf *config.Config) (Backend, error) {
	if cnf.ResultBackend == "" {
		return nil, errors.New("Result backend required")
	}

	for prefix, create := range BackendFactories {
		if strings.HasPrefix(cnf.ResultBackend, prefix) {
			return create(cnf)
		}
	}

	return nil, fmt.Errorf("Factory failed with result backend: %v", cnf.ResultBackend)
}
