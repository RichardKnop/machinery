package machinery_test

import (
	"context"
	"math"
	"testing"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/stretchr/testify/assert"
)

func TestInvalidArgRobustness(t *testing.T) {
	// Create a test task function
	f := func(x int) error { return nil }

	// Construct an invalid argument list and reflect it
	args := []signatures.TaskArg{
		signatures.TaskArg{Type: "bool", Value: true},
	}

	task, err := machinery.NewTask(f, args)
	assert.NoError(t, err)

	// Invoke TryCall and validate error handling
	results, err := task.Call()
	assert.Equal(t, "reflect: Call using bool as type int", err.Error())
	assert.Nil(t, results)
}

func TestInterfaceValuedResult(t *testing.T) {
	// Create a test task function
	f := func() (interface{}, error) { return math.Pi, nil }

	task, err := machinery.NewTask(f, []signatures.TaskArg{})
	assert.NoError(t, err)

	taskResults, err := task.Call()
	assert.NoError(t, err)
	assert.Equal(t, "float64", taskResults[0].Type)
	assert.Equal(t, math.Pi, taskResults[0].Value)
}

func TestTaskHasContext(t *testing.T) {
	f := func(c context.Context) (interface{}, error) {
		assert.NotNil(t, c)
		return math.Pi, nil
	}
	task, err := machinery.NewTask(f, []signatures.TaskArg{})
	assert.NoError(t, err)
	taskResults, err := task.Call()
	assert.NoError(t, err)
	assert.Equal(t, "float64", taskResults[0].Type)
	assert.Equal(t, math.Pi, taskResults[0].Value)
}
