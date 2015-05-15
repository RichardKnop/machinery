package backends

// AsyncResult represents an asynchronous task result
type AsyncResult struct {
	taskUUID string
	backend  Backend
}

// NewAsyncResult creates AsyncResult instance
func NewAsyncResult(taskUUID string, backend Backend) *AsyncResult {
	return &AsyncResult{
		taskUUID: taskUUID,
		backend:  backend,
	}
}
