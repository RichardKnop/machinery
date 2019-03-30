package machinery_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/tasks"
)

func TestOpenTracingPreTaskHandler(t *testing.T) {
	t.Parallel()

	sig := &tasks.Signature{
		Name: "abc",
	}

	ctx := context.Background()
	ctx2 := machinery.OpenTracingPreTaskHandler(ctx, sig)

	assert.NotEqual(t, ctx, ctx2)
}
