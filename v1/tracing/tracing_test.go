package tracing_test

import (
	"context"
	"testing"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/RichardKnop/machinery/v1/tracing"

	"github.com/stretchr/testify/assert"

	"go.opencensus.io/trace"
)

func TestStartSpanFromHeaders(t *testing.T) {
	headers := make(tasks.Headers)
	headers["traceparent"] = "00-BEEFBEEFBEEFBEEFBEEFBEEFBEEFBEEF-CAFECAFECAFECAFE-01"
	ctx, span := tracing.StartSpanFromHeaders(context.Background(), headers, "operation")

	assert.NotNil(t, ctx)
	assert.NotNil(t, span)
	tid := trace.TraceID{0xbe, 0xef, 0xbe, 0xef, 0xbe, 0xef, 0xbe, 0xef, 0xbe, 0xef, 0xbe, 0xef, 0xbe, 0xef, 0xbe, 0xef}
	assert.Equal(t, tid, span.SpanContext().TraceID)
	sid := trace.SpanID{0xca, 0xfe, 0xca, 0xfe, 0xca, 0xfe, 0xca, 0xfe}
	assert.NotEqual(t, sid, span.SpanContext().SpanID)
	assert.Equal(t, trace.TraceOptions(1), span.SpanContext().TraceOptions)
	assert.True(t, span.SpanContext().IsSampled())
}
