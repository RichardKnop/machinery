package v1

// Task is a common interface all registered tasks
// must implement
type Task interface {
	Run(args []interface{}, kwargs map[string]interface{}) interface{}
}

// TaskMessage is a JSON representation of a task
type TaskMessage struct {
	Name   string
	Args   []interface{}
	Kwargs map[string]interface{}
}
