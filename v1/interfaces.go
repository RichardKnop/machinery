package machinery

// Connectable - a common interface for all connections
type Connectable interface {
	Open() Connectable
	Close()
	WaitForMessages(w *Worker)
	PublishMessage(body []byte)
}
