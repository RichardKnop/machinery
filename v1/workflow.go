package machinery

import (
	"fmt"

	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/RichardKnop/uuid"
)

// Chain creates a chain of tasks to be executed one after another
type Chain struct {
	Tasks []*signatures.TaskSignature
}

// Group creates a set of tasks to be executed in parallel
type Group struct {
	GroupUUID string
	Tasks     []*signatures.TaskSignature
}

// Chord adds an optional callback to the group to be executed
// after all tasks in the group finished
type Chord struct {
	Group    *Group
	Callback *signatures.TaskSignature
}

// GetUUIDs returns slice of task UUIDS
func (group *Group) GetUUIDs() []string {
	taskUUIDs := make([]string, len(group.Tasks))
	for i, signature := range group.Tasks {
		taskUUIDs[i] = signature.UUID
	}
	return taskUUIDs
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
		task.UUID = fmt.Sprintf("task_%v", uuid.New())
	}

	return chain
}

// NewGroup creates Group instance
func NewGroup(tasks ...*signatures.TaskSignature) *Group {
	// Generate a group UUID
	groupUUID := fmt.Sprintf("group_%v", uuid.New())

	// Auto generate task UUIDs
	// Group tasks by common UUID
	for _, task := range tasks {
		if task.UUID == "" {
			task.UUID = fmt.Sprintf("task_%v", uuid.New())
		}
		task.GroupUUID = groupUUID
		task.GroupTaskCount = len(tasks)
	}

	return &Group{
		GroupUUID: groupUUID,
		Tasks:     tasks,
	}
}

// NewChord creates Chord instance
func NewChord(group *Group, callback *signatures.TaskSignature) *Chord {
	// Generate a UUID for the chord callback
	callback.UUID = fmt.Sprintf("chord_%v", uuid.New())

	// Add a chord callback to all tasks
	for _, task := range group.Tasks {
		task.ChordCallback = callback
	}

	return &Chord{Group: group, Callback: callback}
}
