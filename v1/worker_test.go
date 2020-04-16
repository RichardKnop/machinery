package machinery

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRedactURL(t *testing.T) {
	t.Parallel()

	broker := "amqp://guest:guest@localhost:5672"
	redactedURL := RedactURL(broker)
	assert.Equal(t, "amqp://localhost:5672", redactedURL)
}
