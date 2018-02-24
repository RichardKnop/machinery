package tasks_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestValidateTask(t *testing.T) {
	t.Parallel()

	type someStruct struct{}
	var (
		taskOfWrongType                   = new(someStruct)
		taskWithoutReturnValue            = func() {}
		taskWithoutErrorAsLastReturnValue = func() int { return 0 }
		validTask                         = func(arg string) error { return nil }
	)

	err := tasks.ValidateTask(taskOfWrongType)
	assert.Equal(t, tasks.ErrTaskMustBeFunc, err)

	err = tasks.ValidateTask(taskWithoutReturnValue)
	assert.Equal(t, tasks.ErrTaskReturnsNoValue, err)

	err = tasks.ValidateTask(taskWithoutErrorAsLastReturnValue)
	assert.Equal(t, tasks.ErrLastReturnValueMustBeError, err)

	err = tasks.ValidateTask(validTask)
	assert.NoError(t, err)
}
