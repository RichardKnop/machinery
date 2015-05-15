package signatures

import "testing"

func TestAdjustRoutingKey(t *testing.T) {
	var signature TaskSignature

	signature = TaskSignature{
		RoutingKey: "routing_key",
	}
	signature.AdjustRoutingKey("direct", "binding_key", "queue")
	if signature.RoutingKey != "routing_key" {
		t.Errorf(
			"signature.RoutingKey = %v, want routing_key",
			signature.RoutingKey,
		)
	}

	signature = TaskSignature{
		RoutingKey: "",
	}
	signature.AdjustRoutingKey("direct", "binding_key", "queue")
	if signature.RoutingKey != "binding_key" {
		t.Errorf(
			"signature.RoutingKey = %v, want binding_key",
			signature.RoutingKey,
		)
	}

	signature = TaskSignature{
		RoutingKey: "",
	}
	signature.AdjustRoutingKey("topic", "binding_key", "queue")
	if signature.RoutingKey != "queue" {
		t.Errorf(
			"signature.RoutingKey = %v, want queue",
			signature.RoutingKey,
		)
	}
}
