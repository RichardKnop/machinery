package retry_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/retry"
	"github.com/stretchr/testify/assert"
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

	assert.EqualValues(t, sequence, []int{1, 1, 2, 3, 5, 8})
}

func TestFibonacciNext(t *testing.T) {
	assert.Equal(t, 1, retry.FibonacciNext(0))
	assert.Equal(t, 2, retry.FibonacciNext(1))
	assert.Equal(t, 5, retry.FibonacciNext(3))
	assert.Equal(t, 5, retry.FibonacciNext(4))
	assert.Equal(t, 8, retry.FibonacciNext(5))
	assert.Equal(t, 13, retry.FibonacciNext(8))
}
