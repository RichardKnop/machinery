/*
 * Example tasks
 * -------------
 *
 * Some example tasks. Each task should implement lib.Task interface.
 * Put your business logic inside the Process method.
 * Task arguments are available in the kwargs map.
 */

package tasks

import "fmt"

// Foobar task
type Foobar struct{}

// Process will be called by a worker process
func (f Foobar) Process(kwargs map[string]interface{}) {
	fmt.Println("Foobar task handler")
	fmt.Printf("Received kwargs: %v\n", kwargs)
}
