package brokers_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/brokers"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestIsTaskRegistered(t *testing.T) {
	broker := brokers.New(new(config.Config))
	broker.SetRegisteredTaskNames([]string{"foo", "bar"})

	assert.True(t, broker.IsTaskRegistered("foo"))
	assert.False(t, broker.IsTaskRegistered("bogus"))
}

func TestAdjustRoutingKey(t *testing.T) {
	var (
		s      *tasks.Signature
		broker brokers.Broker
	)

	t.Run("with routing and binding keys", func(t *testing.T) {
		s := &tasks.Signature{RoutingKey: "routing_key"}
		broker := brokers.NewAMQPBroker(&config.Config{
			DefaultQueue: "queue",
			AMQP: &config.AMQPConfig{
				ExchangeType: "direct",
				BindingKey:   "binding_key",
			},
		})
		brokers.AdjustRoutingKey(broker, s)
		assert.Equal(t, "routing_key", s.RoutingKey)
	})

	t.Run("with routing key", func(t *testing.T) {
		s = &tasks.Signature{RoutingKey: "routing_key"}
		broker = brokers.New(&config.Config{
			DefaultQueue: "queue",
		})
		brokers.AdjustRoutingKey(&broker, s)
		assert.Equal(t, "routing_key", s.RoutingKey)
	})

	t.Run("with binding key", func(t *testing.T) {
		s = new(tasks.Signature)
		amqpBroker := brokers.NewAMQPBroker(&config.Config{
			DefaultQueue: "queue",
			AMQP: &config.AMQPConfig{
				ExchangeType: "direct",
				BindingKey:   "binding_key",
			},
		})
		brokers.AdjustRoutingKey(amqpBroker, s)
		assert.Equal(t, "binding_key", s.RoutingKey)
	})

	t.Run("with neither routing nor binding key", func(t *testing.T) {
		s = new(tasks.Signature)
		broker = brokers.New(&config.Config{
			DefaultQueue: "queue",
		})
		brokers.AdjustRoutingKey(&broker, s)
		assert.Equal(t, "queue", s.RoutingKey)
	})
}
