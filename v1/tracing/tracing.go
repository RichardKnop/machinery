package tracing

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/RichardKnop/machinery/v1/tasks"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// opentracing tags
var (
	MachineryTag       = attribute.String("component", "machinery")
	WorkflowGroupTag   = attribute.String("machinery.workflow", "group")
	WorkflowChordTag   = attribute.String("machinery.workflow", "chord")
	WorkflowChainTag   = attribute.String("machinery.workflow", "chain")
	MachineryTraceName = "machinery"
)

// StartSpanFromHeaders will extract a span from the signature headers
// and start a new span with the given operation name.
func StartSpanFromHeaders(ctx context.Context, headers http.Header, operationName string) (_spanCtx context.Context, _span trace.Span) {
	// Try to extract the span context from the carrier.
	spanContext := otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(headers))

	// Create a new span from the span context if found or start a new trace with the function name.
	// For clarity add the machinery component tag.
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(MachineryTag),
	}
	spanCtx, span := otel.Tracer(MachineryTraceName).Start(spanContext, operationName, opts...)

	return spanCtx, span
}

// HeadersWithSpan will inject a span into the signature headers
func HeadersWithSpan(ctx context.Context, headers http.Header) http.Header {
	// check if the headers aren't nil
	if headers == nil {
		headers = make(http.Header)
	}

	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(headers))

	return headers
}

// AnnotateSpanWithSignatureInfo ...
func AnnotateSpanWithSignatureInfo(span trace.Span, signature *tasks.Signature) {

	attrs := []attribute.KeyValue{
		// tag the span with some info about the signature
		attribute.String("signature.uuid", signature.UUID),
		attribute.String("signature.name", signature.Name),
	}

	if signature.GroupUUID != "" {
		attrs = append(attrs, attribute.String("signature.group.uuid", signature.GroupUUID))
	}

	if signature.ChordCallback != nil {
		attrs = append(attrs, attribute.String("signature.chord.callback.uuid", signature.ChordCallback.UUID))
		attrs = append(attrs, attribute.String("signature.chord.callback.name", signature.ChordCallback.Name))
	}

	span.SetAttributes(attrs...)
}

// AnnotateSpanWithChainInfo ...
func AnnotateSpanWithChainInfo(ctx context.Context, span trace.Span, chain *tasks.Chain) {
	// tag the span with some info about the chain
	span.SetAttributes(attribute.Int("chain.tasks.length", len(chain.Tasks)))

	// inject the tracing span into the tasks signature headers
	for _, signature := range chain.Tasks {
		signature.Headers = HeadersWithSpan(ctx, signature.Headers)
	}
}

// AnnotateSpanWithGroupInfo ...
func AnnotateSpanWithGroupInfo(ctx context.Context, span trace.Span, group *tasks.Group, sendConcurrency int) {
	// tag the span with some info about the group
	attrs := []attribute.KeyValue{
		attribute.String("group.uuid", group.GroupUUID),
		attribute.Int("group.tasks.length", len(group.Tasks)),
		attribute.Int("group.concurrency", sendConcurrency),
	}

	// encode the task uuids to json, if that fails just dump it in
	if taskUUIDs, err := json.Marshal(group.GetUUIDs()); err == nil {
		attrs = append(attrs, attribute.String("group.tasks", string(taskUUIDs)))
	} else {
		attrs = append(attrs, attribute.StringSlice("group.tasks", group.GetUUIDs()))
	}
	span.SetAttributes(attrs...)

	// inject the tracing span into the tasks signature headers
	for _, signature := range group.Tasks {
		signature.Headers = HeadersWithSpan(ctx, signature.Headers)
	}
}

// AnnotateSpanWithChordInfo ...
func AnnotateSpanWithChordInfo(ctx context.Context, span trace.Span, chord *tasks.Chord, sendConcurrency int) {
	// tag the span with chord specific info
	span.SetAttributes(attribute.String("chord.callback.uuid", chord.Callback.UUID))

	// inject the tracing span into the callback signature
	chord.Callback.Headers = HeadersWithSpan(ctx, chord.Callback.Headers)

	// tag the span for the group part of the chord
	AnnotateSpanWithGroupInfo(ctx, span, chord.Group, sendConcurrency)
}
