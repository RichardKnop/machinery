package tasks_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

func TestAdjustRoutingKey(t *testing.T) {
	var signature tasks.Signature

	signature = tasks.Signature{
		RoutingKey: "routing_key",
	}
	signature.AdjustRoutingKey("direct", "binding_key", "queue")

	assert.Equal(t, "routing_key", signature.RoutingKey)

	signature = tasks.Signature{
		RoutingKey: "",
	}
	signature.AdjustRoutingKey("direct", "binding_key", "queue")

	assert.Equal(t, "binding_key", signature.RoutingKey)

	signature = tasks.Signature{
		RoutingKey: "",
	}
	signature.AdjustRoutingKey("topic", "binding_key", "queue")

	assert.Equal(t, "queue", signature.RoutingKey)
}
