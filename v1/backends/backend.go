package backends

import (
	"github.com/RichardKnop/machinery/v1/config"
)

// Broker represents a base backend structure
type Backend struct {
	cnf *config.Config
}

// New creates new Backend instance
func New(cnf *config.Config) Backend {
	return Backend{cnf: cnf}
}

// IsAMQP returns true if the backend is AMQP
func IsAMQP(backend Interface) bool {
	_, isAMQPBackend := backend.(*AMQPBackend)
	return isAMQPBackend
}
