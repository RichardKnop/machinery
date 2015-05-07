// Package exampletasks - some sample tasks
// Each task should implement v1.Task interface.
// Put your business logic inside the Run method.
// Task arguments are available in the args slice.
package exampletasks

import machinery "github.com/RichardKnop/machinery/v1"

// AddTask ...
type AddTask struct{}

// Run - runs the task and returns sum of all numbers in args
func (t AddTask) Run(args []interface{}) (interface{}, error) {
	parsedArgs, err := machinery.ParseNumberArgs(args)
	if err != nil {
		return nil, err
	}

	add := func(args []float64) float64 {
		sum := 0.0
		for _, arg := range args {
			sum += arg
		}
		return sum
	}

	return add(parsedArgs), nil
}

// MultiplyTask ...
type MultiplyTask struct{}

// Run - runs the task and returns result of all numbers in args multiplied
func (t MultiplyTask) Run(args []interface{}) (interface{}, error) {
	parsedArgs, err := machinery.ParseNumberArgs(args)
	if err != nil {
		return nil, err
	}

	multiply := func(args []float64) float64 {
		sum := 1.0
		for _, arg := range args {
			sum *= arg
		}
		return sum
	}

	return multiply(parsedArgs), nil
}
