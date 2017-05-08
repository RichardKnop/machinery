package tasks

import (
	"fmt"

	"github.com/RichardKnop/uuid"
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

// NewChain creates Chain instance
func NewChain(signatures ...*Signature) *Chain {
	for i := len(signatures) - 1; i > 0; i-- {
		if i > 0 {
			signatures[i-1].OnSuccess = []*Signature{signatures[i]}
		}
	}

	chain := &Chain{Tasks: signatures}

	// Auto generate task UUIDs
	for _, signature := range chain.Tasks {
		signature.UUID = fmt.Sprintf("task_%v", uuid.New())
	}

	return chain
}

// NewGroup creates Group instance
func NewGroup(signatures ...*Signature) *Group {
	// Generate a group UUID
	groupUUID := fmt.Sprintf("group_%v", uuid.New())

	// Auto generate task UUIDs
	// Group tasks by common UUID
	for _, signature := range signatures {
		if signature.UUID == "" {
			signature.UUID = fmt.Sprintf("task_%v", uuid.New())
		}
		signature.GroupUUID = groupUUID
		signature.GroupTaskCount = len(signatures)
	}

	return &Group{
		GroupUUID: groupUUID,
		Tasks:     signatures,
	}
}

// NewChord creates Chord instance
func NewChord(group *Group, callback *Signature) *Chord {
	// Generate a UUID for the chord callback
	callback.UUID = fmt.Sprintf("chord_%v", uuid.New())

	// Add a chord callback to all tasks
	for _, signature := range group.Tasks {
		signature.ChordCallback = callback
	}

	return &Chord{Group: group, Callback: callback}
}
