package machinery

// Task is a common interface all registered tasks
// must implement
type Task interface {
	Run(args []interface{}) (interface{}, error)
}

// TaskSignature represents a single task invocation
type TaskSignature struct {
	Name, RoutingKey string
	Args             []interface{}
	Immutable        bool
	OnSuccess        []*TaskSignature
	OnError          []*TaskSignature
}
