// Package exampletasks - some sample tasks
// Each task should implement v1.Task interface.
// Put your business logic inside the Process method.
// Task arguments are available in the kwargs map.
package exampletasks

import "log"

// AddTask task
type AddTask struct{}

// Run will be called by a worker process
func (f AddTask) Run(
	args []interface{}, kwargs map[string]interface{},
) interface{} {
	sum := 0.0
	for _, arg := range args {
		n, ok := arg.(float64)
		if !ok {
			continue
		}
		sum += n
	}
	log.Printf("%v", sum)
	return sum
}
