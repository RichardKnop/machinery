package tasks

import (
	"fmt"

	"github.com/satori/go.uuid"
)

// Chain creates a chain of tasks to be executed one after another
type Chain struct {
	Tasks []*Signature
}

// Group creates a set of tasks to be executed in parallel
type Group struct {
	GroupUUID string
	Tasks     []*Signature
}

// Chord adds an optional callback to the group to be executed
// after all tasks in the group finished
type Chord struct {
	Group    *Group
	Callback *Signature
}

// GetUUIDs returns slice of task UUIDS
func (group *Group) GetUUIDs() []string {
	taskUUIDs := make([]string, len(group.Tasks))
	for i, signature := range group.Tasks {
		taskUUIDs[i] = signature.UUID
	}
	return taskUUIDs
}

// NewChain creates a new chain of tasks to be processed one by one, passing
// results unless task signatures are set to be immutable
func NewChain(signatures ...*Signature) *Chain {
	// Auto generate task UUIDs if needed
	for _, signature := range signatures {
		if signature.UUID == "" {
			signature.UUID = fmt.Sprintf("task_%v", uuid.NewV4())
		}
	}

	for i := len(signatures) - 1; i > 0; i-- {
		if i > 0 {
			signatures[i-1].OnSuccess = []*Signature{signatures[i]}
		}
	}

	chain := &Chain{Tasks: signatures}

	return chain
}

// NewGroup creates a new group of tasks to be processed in parallel
func NewGroup(signatures ...*Signature) *Group {
	// Generate a group UUID
	groupUUID := fmt.Sprintf("group_%v", uuid.NewV4())

	// Auto generate task UUIDs if needed, group tasks by common group UUID
	for _, signature := range signatures {
		if signature.UUID == "" {
			signature.UUID = fmt.Sprintf("task_%v", uuid.NewV4())
		}
		signature.GroupUUID = groupUUID
		signature.GroupTaskCount = len(signatures)
	}

	return &Group{
		GroupUUID: groupUUID,
		Tasks:     signatures,
	}
}

// NewChord creates a new chord (a group of tasks with a single callback
// to be executed after all tasks in the group has completed)
func NewChord(group *Group, callback *Signature) *Chord {
	// Generate a UUID for the chord callback
	callback.UUID = fmt.Sprintf("chord_%v", uuid.NewV4())

	// Add a chord callback to all tasks
	for _, signature := range group.Tasks {
		signature.ChordCallback = callback
	}

	return &Chord{Group: group, Callback: callback}
}
