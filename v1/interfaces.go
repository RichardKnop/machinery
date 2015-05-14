package machinery

// Connectable - a common interface for all connections
type Connectable interface {
	Consume(w *Worker) error
	Publish(body []byte, routingKey string) error
}
