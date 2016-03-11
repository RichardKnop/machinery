package machinery

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/signatures"
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

	chain := NewChain(&task1, &task2, &task3)

	firstTask := chain.Tasks[0]

	if firstTask.Name != "foo" {
		t.Errorf("firstTask.Name = %v, want foo", firstTask.Name)
	}

	if firstTask.OnSuccess[0].Name != "bar" {
		t.Errorf(
			"firstTask.OnSuccess[0].Name = %v, want bar",
			firstTask.OnSuccess[0].Name,
		)
	}

	if firstTask.OnSuccess[0].OnSuccess[0].Name != "qux" {
		t.Errorf(
			"firstTask.OnSuccess[0].OnSuccess[0].Name = %v, want qux",
			firstTask.OnSuccess[0].OnSuccess[0].Name,
		)
	}
}
