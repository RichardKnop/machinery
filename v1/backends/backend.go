package backends

import (
	"github.com/RichardKnop/machinery/v1/config"
)

// Backend represents a base backend structure
type Backend struct {
	cnf *config.Config
}

// New creates new Backend instance
func New(cnf *config.Config) Backend {
	return Backend{cnf: cnf}
}

// IsAMQP returns true if the backend is AMQP
func IsAMQP(b Interface) bool {
	_, isAMQPBackend := b.(*AMQPBackend)
	return isAMQPBackend
}
