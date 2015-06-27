package machinery

import (
	"code.google.com/p/go-uuid/uuid"
	"github.com/RichardKnop/machinery/v1/signatures"
)

// Chain creates a chain of tasks to be executed one after another
type Chain struct {
	Tasks []*signatures.TaskSignature
}

// Group creates a set of tasks to be executed in parallel
type Group struct {
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

	// Auto generate task UUIDs
	for _, task := range chain.Tasks {
		task.UUID = uuid.New()
	}

	return chain
}

// NewGroup creates Group instance
func NewGroup(tasks ...*signatures.TaskSignature) *Group {
	// Generate a group UUID
	groupUUID := uuid.New()

	// Auto generate task UUIDs
	// Group tasks by common UUID
	for _, task := range tasks {
		task.UUID = uuid.New()
		task.GroupUUID = groupUUID
	}

	return &Group{Tasks: tasks}
}
