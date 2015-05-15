package backends

// Backend - a common interface for all result backends
type Backend interface {
	UpdateState(taskState *TaskState) error
	GetState(taskUUID string) (*TaskState, error)
}
