package signatures

// TaskArg represents a single argument passed to invocation fo a task
type TaskArg struct {
	Type  string
	Value interface{}
}

// TaskSignature represents a single task invocation
type TaskSignature struct {
	UUID, Name, RoutingKey string
	Args                   []TaskArg
	Immutable              bool
	OnSuccess              []*TaskSignature
	OnError                []*TaskSignature
}
