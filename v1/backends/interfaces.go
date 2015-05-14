package backends

// Backend - a common interface for all result backends
type Backend interface {
	UpdateState(taskUUID, state string) error
}
