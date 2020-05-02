package machinery_test

import (
	"github.com/RichardKnop/machinery/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPreConsumeHandler(t *testing.T) {
	t.Parallel()
	worker := &machinery.Worker{}

	worker.SetPreConsumeHandler(SamplePreConsumeHandler)
	assert.True(t, worker.PreConsumeHandler())
}

func SamplePreConsumeHandler(w *machinery.Worker) bool {
	return true
}