package machinery_test

import (
	"testing"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/stretchr/testify/assert"
)

func TestNewChain(t *testing.T) {
	task1 := signatures.TaskSignature{
		Name: "foo",
		Args: []signatures.TaskArg{
			{
				Type:  "float64",
				Value: interface{}(1),
			},
			{
				Type:  "float64",
				Value: interface{}(1),
			},
		},
	}

	task2 := signatures.TaskSignature{
		Name: "bar",
		Args: []signatures.TaskArg{
			{
				Type:  "float64",
				Value: interface{}(5),
			},
			{
				Type:  "float64",
				Value: interface{}(6),
			},
		},
	}

	task3 := signatures.TaskSignature{
		Name: "qux",
		Args: []signatures.TaskArg{
			{
				Type:  "float64",
				Value: interface{}(4),
			},
		},
	}

	chain := machinery.NewChain(&task1, &task2, &task3)

	firstTask := chain.Tasks[0]

	assert.Equal(t, "foo", firstTask.Name)
	assert.Equal(t, "bar", firstTask.OnSuccess[0].Name)
	assert.Equal(t, "qux", firstTask.OnSuccess[0].OnSuccess[0].Name)
}
