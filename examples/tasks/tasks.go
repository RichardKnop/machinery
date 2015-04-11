// Package exampletasks - some sample tasks
// Each task should implement v1.Task interface.
// Put your business logic inside the Process method.
// Task arguments are available in the kwargs map.
package exampletasks

import (
	"errors"
	"fmt"
)

// Add task
type Add struct{}

// Run adds all numbers in args together and returns their sum
func (t Add) Run(
	args []interface{}, kwargs map[string]interface{},
) (interface{}, error) {
	var err error
	var sum float64

	for _, arg := range args {
		n, ok := arg.(float64)
		if !ok {
			errMsg := fmt.Sprintf("%v not a valid floating point number", n)
			err = errors.New(errMsg)
			break
		}
		sum += n
	}

	return sum, err
}

// Multiply task
type Multiply struct{}

// Run multiplies all numbers in args and returns the result
func (t Multiply) Run(
	args []interface{}, kwargs map[string]interface{},
) (interface{}, error) {
	var err error
	var sum = 1.0

	for _, arg := range args {
		n, ok := arg.(float64)
		if !ok {
			errMsg := fmt.Sprintf("%v not a valid floating point number", n)
			err = errors.New(errMsg)
			break
		}
		sum *= n
	}

	return sum, err
}
