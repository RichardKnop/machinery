package tasks_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestReflectTaskResults(t *testing.T) {
	t.Parallel()

	taskResults := []*tasks.TaskResult{
		{
			Type:  "[]string",
			Value: []string{"f", "o", "o"},
		},
	}
	results, err := tasks.ReflectTaskResults(taskResults)
	if assert.NoError(t, err) {
		assert.Equal(t, 1, len(results))
		assert.Equal(t, 3, results[0].Len())
		assert.Equal(t, "f", results[0].Index(0).String())
		assert.Equal(t, "o", results[0].Index(1).String())
		assert.Equal(t, "o", results[0].Index(2).String())
	}
}
