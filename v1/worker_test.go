package machinery_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1"

	"github.com/stretchr/testify/assert"

	"github.com/RichardKnop/machinery/v1/tasks"
)

func TestProcess(t *testing.T) {
	t.Parallel()

	t.Run("use default task caller", func(t *testing.T) {
		callCount := 0
		server := getTestServer(t)

		dummyTask := func(a uint64) (uint64, error) {
			callCount++
			return a + 1, nil
		}
		assert.NoError(t, server.RegisterTask("Dummy", dummyTask))

		worker := server.NewWorker("test_worker", 1)

		err := worker.Process(&tasks.Signature{
			Name: "Dummy",
			Args: []tasks.Arg{{Type: "uint64", Value: uint64(3)}},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, callCount)
	})

	t.Run("use custom task caller", func(t *testing.T) {
		middlewareCallCount := 0
		callCount := 0
		server := getTestServer(t)

		dummyTask := func(a uint64) (uint64, error) {
			callCount++
			return a + 1, nil
		}
		assert.NoError(t, server.RegisterTask("Dummy", dummyTask))

		worker := server.NewWorker("test_worker", 1)
		worker.SetTaskCaller(machinery.TaskCallerFunc(func(signature *tasks.Signature, task *tasks.Task) (results []*tasks.TaskResult, e error) {
			middlewareCallCount++

			if signature.Args[0].Value == uint64(1000) {
				// Abort task processing.
				return nil, nil
			}

			return machinery.DefaultTaskCaller.Call(signature, task)
		}))

		assert.NoError(t, worker.Process(&tasks.Signature{
			Name: "Dummy",
			Args: []tasks.Arg{{Type: "uint64", Value: uint64(3)}},
		}))
		assert.Equal(t, 1, middlewareCallCount)
		assert.Equal(t, 1, callCount)

		assert.NoError(t, worker.Process(&tasks.Signature{
			Name: "Dummy",
			Args: []tasks.Arg{{Type: "uint64", Value: uint64(1000)}},
		}))
		assert.Equal(t, 2, middlewareCallCount)
		assert.Equal(t, 1, callCount)
	})

	t.Run("use custom panic handler", func(t *testing.T) {
		handledPanic := false
		server := getTestServer(t)

		dummyTask := func(a uint64) (uint64, error) {
			panic("foo")
		}
		assert.NoError(t, server.RegisterTask("Dummy", dummyTask))

		worker := server.NewWorker("test_worker", 1)
		worker.SetPanicHandler(func(task *tasks.Task, err interface{}) {
			handledPanic = true
			assert.Equal(t, "foo", err)
		})

		assert.NoError(t, worker.Process(&tasks.Signature{
			Name: "Dummy",
			Args: []tasks.Arg{{Type: "uint64", Value: uint64(3)}},
		}))
		assert.True(t, handledPanic)
	})
}
