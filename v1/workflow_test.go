package machinery

import "testing"

func TestChain(t *testing.T) {
	task1 := TaskSignature{
		Name: "foo",
		Args: []interface{}{1, 1},
	}

	task2 := TaskSignature{
		Name: "bar",
		Args: []interface{}{5, 6},
	}

	task3 := TaskSignature{
		Name: "qux",
		Args: []interface{}{4},
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
