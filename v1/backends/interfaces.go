package backends

// Backend - a common interface for all result backends
type Backend interface {
	UpdateState(taskUUID, state string, result *TaskResult) error
	GetState(taskUUID string) (*TaskState, error)
}
