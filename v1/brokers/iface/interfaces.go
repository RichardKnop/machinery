package iface

import (
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// Broker - a common interface for all brokers
type Broker interface {
	GetConfig() *config.Config
	SetRegisteredTaskNames(names []string)
	IsTaskRegistered(name string) bool
	StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error)
	StopConsuming()
	Publish(task *tasks.Signature) error
	GetPendingTasks(queue string) ([]*tasks.Signature, error)
	AdjustRoutingKey(s *tasks.Signature)
}

// TaskProcessor - can process a delivered task
// This will probably always be a worker instance
type TaskProcessor interface {
	Process(signature *tasks.Signature) error
	CustomQueue() string
}
