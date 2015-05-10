package machinery

// Connectable - a common interface for all connections
type Connectable interface {
	Open() (Connectable, error)
	Close() error
	WaitForMessages(w *Worker)
	PublishMessage(body []byte, routingKey string) error
}
