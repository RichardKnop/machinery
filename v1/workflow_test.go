package machinery

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/signatures"
)

func TestChain(t *testing.T) {
	task1 := signatures.TaskSignature{
		Name: "foo",
		Args: []signatures.TaskArg{
			signatures.TaskArg{
				Type:  "float64",
				Value: interface{}(1),
			},
			signatures.TaskArg{
				Type:  "float64",
				Value: interface{}(1),
			},
		},
	}

	task2 := signatures.TaskSignature{
		Name: "bar",
		Args: []signatures.TaskArg{
			signatures.TaskArg{
				Type:  "float64",
				Value: interface{}(5),
			},
			signatures.TaskArg{
				Type:  "float64",
				Value: interface{}(6),
			},
		},
	}

	task3 := signatures.TaskSignature{
		Name: "qux",
		Args: []signatures.TaskArg{
			signatures.TaskArg{
				Type:  "float64",
				Value: interface{}(4),
			},
		},
	}

	chain := Chain(task1, task2, task3)

	if chain.Name != "foo" {
		t.Errorf("chain.Name = %v, want foo", chain.Name)
	}

	if chain.OnSuccess[0].Name != "bar" {
		t.Errorf(
			"chain.OnSuccess[0].Name = %v, want bar",
			chain.OnSuccess[0].Name,
		)
	}

	if chain.OnSuccess[0].OnSuccess[0].Name != "qux" {
		t.Errorf(
			"chain.OnSuccess[0].OnSuccess[0].Name = %v, want qux",
			chain.OnSuccess[0].OnSuccess[0].Name,
		)
	}
}
