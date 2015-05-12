package machinery

// Connectable - a common interface for all connections
type Connectable interface {
	WaitForMessages(w *Worker) error
	PublishMessage(body []byte, routingKey string) error
}
