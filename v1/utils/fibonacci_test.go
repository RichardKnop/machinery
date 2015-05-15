package utils

import (
	"reflect"
	"testing"
)

func TestFibonacci(t *testing.T) {
	fibonacci := Fibonacci()

	sequence := []int{
		fibonacci(),
		fibonacci(),
		fibonacci(),
		fibonacci(),
		fibonacci(),
		fibonacci(),
	}

	if !reflect.DeepEqual(sequence, []int{1, 1, 2, 3, 5, 8}) {
		t.Errorf("sequence = %v, want [1, 1, 2, 3, 5, 8]", sequence)
	}
}
