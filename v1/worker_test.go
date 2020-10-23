package machinery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/RichardKnop/machinery/v1"
)

func TestRedactURL(t *testing.T) {
	t.Parallel()

	broker := "amqp://guest:guest@localhost:5672"
	redactedURL := machinery.RedactURL(broker)
	assert.Equal(t, "amqp://localhost:5672", redactedURL)
}

func TestPreConsumeHandler(t *testing.T) {
	t.Parallel()
	
	worker := &machinery.Worker{}

	worker.SetPreConsumeHandler(SamplePreConsumeHandler)
	assert.True(t, worker.PreConsumeHandler())
}

func SamplePreConsumeHandler(w *machinery.Worker) bool {
	return true
}
