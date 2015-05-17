package machinery

import (
	"code.google.com/p/go-uuid/uuid"
	"github.com/RichardKnop/machinery/v1/signatures"
)

// Chain creates a chain of tasks to be executed one after another
type Chain struct {
	Tasks []*signatures.TaskSignature
}

// NewChain creates Chain instance
func NewChain(tasks ...*signatures.TaskSignature) *Chain {
	for i := len(tasks) - 1; i > 0; i-- {
		if i > 0 {
			tasks[i-1].OnSuccess = []*signatures.TaskSignature{tasks[i]}
		}
	}

	chain := &Chain{Tasks: tasks}

	// Auto generate a UUIDs if not set already
	for _, task := range chain.Tasks {
		task.UUID = uuid.New()
	}

	return chain
}

// GetUUIDs returns []string of task UUIDs
func (chain *Chain) GetUUIDs() []string {
	UUIDs := make([]string, len(chain.Tasks))
	for i, task := range chain.Tasks {
		UUIDs[i] = task.UUID
	}
	return UUIDs
}
