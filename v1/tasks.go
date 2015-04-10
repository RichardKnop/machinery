package v1

// Task is a common interface all registered tasks
// must implement
type Task interface {
	Run(args []interface{}, kwargs map[string]interface{}) interface{}
}

// TaskSignature represents a single task invocation
type TaskSignature struct {
	Name       string
	Args       []interface{}
	Kwargs     map[string]interface{}
	Subsequent []TaskSignature
}
