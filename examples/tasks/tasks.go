// Package exampletasks - some sample tasks
// Each task should implement v1.Task interface.
// Put your business logic inside the Process method.
// Task arguments are available in the kwargs map.
package exampletasks

import "fmt"

// Foo task
type Foo struct{}

// Run will be called by a worker process
func (f Foo) Run(args []interface{}, kwargs map[string]interface{}) {
	fmt.Println("Foo task is being processed")
	fmt.Printf("Received args: %v\n", args)
	fmt.Printf("Received kwargs: %v\n", kwargs)
}

// AddTask task
type AddTask struct{}

// Run will be called by a worker process
func (f AddTask) Run(args []interface{}, kwargs map[string]interface{}) {
	fmt.Println("Foobar task handler")
	fmt.Printf("Received kwargs: %v\n", kwargs)
}
