package machinery

// Chain - creates a chain of tasks to be executed one after another
func Chain(signatures ...TaskSignature) *TaskSignature {
	for i := len(signatures) - 1; i > 0; i-- {
		if i > 0 {
			signatures[i-1].OnSuccess = []*TaskSignature{&signatures[i]}
		}
	}

	return &signatures[0]
}
