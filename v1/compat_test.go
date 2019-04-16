package machinery_test

import (
	"context"

	"testing"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/tasks"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
)

func TestOpenTracingPreTaskHandler(t *testing.T) {
	t.Parallel()

	sig := &tasks.Signature{
		Name: "abc",
	}

	ctx := context.Background()
	ctx2 := machinery.OpenTracingPreTaskHandler(ctx, sig)

	assert.NotEqual(t, ctx, ctx2, "OpenTracingPreTaskHandler returns a new context")
	assert.NotNil(t, opentracing.SpanFromContext(ctx2), "context returned contains an opentracing span")
}

func TestOpenTracingPostTaskHandler(t *testing.T) {
	t.Parallel()

	mockTracer := mocktracer.New()
	span := mockTracer.StartSpan("new task")
	mockSpan := span.(*mocktracer.MockSpan)

	assert.Zero(t, mockSpan.FinishTime, "span not finished")

	sig := &tasks.Signature{
		Name: "abc",
	}

	ctx := opentracing.ContextWithSpan(context.Background(), span)
	machinery.OpenTracingPostTaskHandler(ctx, sig)

	assert.NotZero(t, mockSpan.FinishTime, "span finished")
}

func TestOpenTracingPrePublishHandlerAddsHeaders(t *testing.T) {

	globalTracer := opentracing.GlobalTracer()

	defer func() {
		opentracing.SetGlobalTracer(globalTracer)
	}()

	sig := &tasks.Signature{
		Name: "abc",
	}

	mockTracer := mocktracer.New()
	opentracing.SetGlobalTracer(mockTracer)

	span := mockTracer.StartSpan("new task")
	ctx := opentracing.ContextWithSpan(context.Background(), span)

	assert.Empty(t, sig.Headers)

	f := machinery.OpenTracingPrePublishTaskHandler(ctx, sig)

	assert.NotNil(t, f)

	assert.NotEmpty(t, sig.Headers, "signature headers populated from tracing call")
}
