package machinery

import "github.com/RichardKnop/machinery/v1/signatures"

// Chain - creates a chain of tasks to be executed one after another
func Chain(tasks ...signatures.TaskSignature) *signatures.TaskSignature {
	for i := len(tasks) - 1; i > 0; i-- {
		if i > 0 {
			tasks[i-1].OnSuccess = []*signatures.TaskSignature{&tasks[i]}
		}
	}

	return &tasks[0]
}
