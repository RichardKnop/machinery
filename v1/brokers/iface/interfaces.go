package iface

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
	Publish(ctx context.Context, task *tasks.Signature) error
	GetPendingTasks(queue string) ([]*tasks.Signature, error)
	AdjustRoutingKey(s *tasks.Signature)
}

// TaskProcessor - can process a delivered task
// This will probably always be a worker instance
type TaskProcessor interface {
	Process(signature *tasks.Signature) error
	CustomQueue() string
}

var BrokerFactories = map[string]func(*config.Config) (Broker, error){}

// BrokerFactory creates a new object of iface.Broker
// Currently only AMQP/S broker is supported
func BrokerFactory(cnf *config.Config) (Broker, error) {
	if cnf.Broker == "" {
		return nil, errors.New("broker required")
	}

	for prefix, create := range BrokerFactories {
		if strings.HasPrefix(cnf.Broker, prefix) {
			return create(cnf)
		}
	}
	return nil, fmt.Errorf("Factory failed with broker URL: %v", cnf.Broker)
}
