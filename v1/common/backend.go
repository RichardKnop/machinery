package common

import (
	"github.com/RichardKnop/machinery/v1/config"
)

// Backend represents a base backend structure
type Backend struct {
	cnf *config.Config
}

// NewBackend creates new Backend instance
func NewBackend(cnf *config.Config) Backend {
	return Backend{cnf: cnf}
}

// GetConfig returns config
func (b *Backend) GetConfig() *config.Config {
	return b.cnf
}

// IsAMQP ...
func (b *Backend) IsAMQP() bool {
	return false
}
