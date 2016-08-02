package signatures_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/stretchr/testify/assert"
)

func TestAdjustRoutingKey(t *testing.T) {
	var signature signatures.TaskSignature

	signature = signatures.TaskSignature{
		RoutingKey: "routing_key",
	}
	signature.AdjustRoutingKey("direct", "binding_key", "queue")

	assert.Equal(t, "routing_key", signature.RoutingKey)

	signature = signatures.TaskSignature{
		RoutingKey: "",
	}
	signature.AdjustRoutingKey("direct", "binding_key", "queue")

	assert.Equal(t, "binding_key", signature.RoutingKey)

	signature = signatures.TaskSignature{
		RoutingKey: "",
	}
	signature.AdjustRoutingKey("topic", "binding_key", "queue")

	assert.Equal(t, "queue", signature.RoutingKey)
}
