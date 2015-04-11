// Package exampletasks - some sample tasks
// Each task should implement v1.Task interface.
// Put your business logic inside the Process method.
// Task arguments are available in the kwargs map.
package exampletasks

import "errors"

// AddTask task
type AddTask struct{}

// Run will be called by a worker process
func (f AddTask) Run(
	args []interface{}, kwargs map[string]interface{},
) (interface{}, error) {
	var err error
	sum := 0.0

	for _, arg := range args {
		n, ok := arg.(float64)
		if !ok {
			err = errors.New("Argument not a valid number")
			break
		}
		sum += n
	}

	return sum, err
}
