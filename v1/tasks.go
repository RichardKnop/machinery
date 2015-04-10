package v1

// Task is a common interface all registered tasks
// must implement
type Task interface {
	Process(kwargs map[string]interface{})
}
