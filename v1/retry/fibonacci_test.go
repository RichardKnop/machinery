package retry_test

import (
	"reflect"
	"testing"

	"github.com/RichardKnop/machinery/v1/retry"
)

func TestFibonacci(t *testing.T) {
	fibonacci := retry.Fibonacci()

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
