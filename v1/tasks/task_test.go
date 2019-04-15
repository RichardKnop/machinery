package tasks_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestTaskCallErrorTest(t *testing.T) {
	t.Parallel()

	// Create test task that returns tasks.ErrRetryTaskLater error
	retriable := func() error { return tasks.NewErrRetryTaskLater("some error", 4*time.Hour) }

	task, err := tasks.New(retriable, []tasks.Arg{})
	assert.NoError(t, err)

	// Invoke TryCall and validate that returned error can be cast to tasks.ErrRetryTaskLater
	results, err := task.Call()
	assert.Nil(t, results)
	assert.NotNil(t, err)
	_, ok := interface{}(err).(tasks.ErrRetryTaskLater)
	assert.True(t, ok, "Error should be castable to tasks.ErrRetryTaskLater")

	// Create test task that returns a standard error
	standard := func() error { return errors.New("some error") }

	task, err = tasks.New(standard, []tasks.Arg{})
	assert.NoError(t, err)

	// Invoke TryCall and validate that returned error is standard
	results, err = task.Call()
	assert.Nil(t, results)
	assert.NotNil(t, err)
	assert.Equal(t, "some error", err.Error())
}

func TestTaskReflectArgs(t *testing.T) {
	t.Parallel()

	task := new(tasks.Task)
	args := []tasks.Arg{
		{
			Type:  "[]int64",
			Value: []int64{1, 2},
		},
	}

	err := task.ReflectArgs(args)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(task.Args))
	assert.Equal(t, "[]int64", task.Args[0].Type().String())
}

func TestTaskCallInvalidArgRobustnessError(t *testing.T) {
	t.Parallel()

	// Create a test task function
	f := func(x int) error { return nil }

	// Construct an invalid argument list and reflect it
	args := []tasks.Arg{
		{Type: "bool", Value: true},
	}

	task, err := tasks.New(f, args)
	assert.NoError(t, err)

	// Invoke TryCall and validate error handling
	results, err := task.Call()
	assert.Equal(t, "reflect: Call using bool as type int", err.Error())
	assert.Nil(t, results)
}

func TestTaskCallInterfaceValuedResult(t *testing.T) {
	t.Parallel()

	// Create a test task function
	f := func() (interface{}, error) { return math.Pi, nil }

	task, err := tasks.New(f, []tasks.Arg{})
	assert.NoError(t, err)

	taskResults, err := task.Call()
	assert.NoError(t, err)
	assert.Equal(t, "float64", taskResults[0].Type)
	assert.Equal(t, math.Pi, taskResults[0].Value)
}

func TestTaskCallWithContext(t *testing.T) {
	t.Parallel()

	f := func(c context.Context) (interface{}, error) {
		assert.NotNil(t, c)
		assert.Nil(t, tasks.SignatureFromContext(c))
		return math.Pi, nil
	}
	task, err := tasks.New(f, []tasks.Arg{})
	assert.NoError(t, err)
	taskResults, err := task.Call()
	assert.NoError(t, err)
	assert.Equal(t, "float64", taskResults[0].Type)
	assert.Equal(t, math.Pi, taskResults[0].Value)
}

func TestTaskCallWithSignatureInContext(t *testing.T) {
	t.Parallel()

	f := func(c context.Context) (interface{}, error) {
		assert.NotNil(t, c)
		signature := tasks.SignatureFromContext(c)
		assert.NotNil(t, signature)
		assert.Equal(t, "foo", signature.Name)
		return math.Pi, nil
	}
	signature, err := tasks.NewSignature("foo", []tasks.Arg{})
	assert.NoError(t, err)
	task, err := tasks.NewWithSignature(f, signature)
	assert.NoError(t, err)
	taskResults, err := task.Call()
	assert.NoError(t, err)
	assert.Equal(t, "float64", taskResults[0].Type)
	assert.Equal(t, math.Pi, taskResults[0].Value)
}
