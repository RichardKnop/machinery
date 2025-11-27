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
		{
			Type:  "int",
			Value: 0,
		},
		{
			Type:  "int32",
			Value: 1,
		},
		{
			Type:  "int64",
			Value: 2,
		},
		{
			Type:  "float32",
			Value: float32(0.1),
		},
		{
			Type:  "float64",
			Value: 0.2,
		},
	}
	results, err := tasks.ReflectTaskResults(taskResults)
	if assert.NoError(t, err) {
		assert.Equal(t, 6, len(results))
		assert.Equal(t, 3, results[0].Len())
		assert.Equal(t, "f", results[0].Index(0).String())
		assert.Equal(t, "o", results[0].Index(1).String())
		assert.Equal(t, "o", results[0].Index(2).String())
		assert.Equal(t, 0, int(results[1].Int()))
		assert.Equal(t, int32(1), int32(results[2].Int()))
		assert.Equal(t, 2, int(results[3].Int()))
		assert.Equal(t, float32(0.1), float32(results[4].Float()))
		assert.Equal(t, 0.2, results[5].Float())
	}
}