package tracing

import (
	"encoding/json"

	"github.com/RichardKnop/machinery/v1/tasks"

	opentracing "github.com/opentracing/opentracing-go"
	opentracing_ext "github.com/opentracing/opentracing-go/ext"
	opentracing_log "github.com/opentracing/opentracing-go/log"
)

// opentracing tags
var (
	MachineryTag     = opentracing.Tag{Key: string(opentracing_ext.Component), Value: "machinery"}
	WorkflowGroupTag = opentracing.Tag{Key: "machinery.workflow", Value: "group"}
	WorkflowChordTag = opentracing.Tag{Key: "machinery.workflow", Value: "chord"}
	WorkflowChainTag = opentracing.Tag{Key: "machinery.workflow", Value: "chain"}
)

// StartSpanFromHeaders will extract a span from the signature headers
// and start a new span with the given operation name.
func StartSpanFromHeaders(headers tasks.Headers, operationName string) opentracing.Span {
	// Try to extract the span context from the carrier.
	spanContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, headers)

	// Create a new span from the span context if found or start a new trace with the function name.
	// For clarity add the machinery component tag.
	span := opentracing.StartSpan(
		operationName,
		ConsumerOption(spanContext),
		MachineryTag,
	)

	// Log any error but don't fail
	if err != nil {
		span.LogFields(opentracing_log.Error(err))
	}

	return span
}

// HeadersWithSpan will inject a span into the signature headers
func HeadersWithSpan(headers tasks.Headers, span opentracing.Span) tasks.Headers {
	// check if the headers aren't nil
	if headers == nil {
		headers = make(tasks.Headers)
	}

	if err := opentracing.GlobalTracer().Inject(span.Context(), opentracing.TextMap, headers); err != nil {
		span.LogFields(opentracing_log.Error(err))
	}

	return headers
}

type consumerOption struct {
	producerContext opentracing.SpanContext
}

func (c consumerOption) Apply(o *opentracing.StartSpanOptions) {
	if c.producerContext != nil {
		opentracing.FollowsFrom(c.producerContext).Apply(o)
	}
	opentracing_ext.SpanKindConsumer.Apply(o)
}

// ConsumerOption ...
func ConsumerOption(producer opentracing.SpanContext) opentracing.StartSpanOption {
	return consumerOption{producer}
}

type producerOption struct{}

func (p producerOption) Apply(o *opentracing.StartSpanOptions) {
	opentracing_ext.SpanKindProducer.Apply(o)
}

// ProducerOption ...
func ProducerOption() opentracing.StartSpanOption {
	return producerOption{}
}

// AnnotateSpanWithSignatureInfo ...
func AnnotateSpanWithSignatureInfo(span opentracing.Span, signature *tasks.Signature) {
	// tag the span with some info about the signature
	span.SetTag("signature.name", signature.Name)
	span.SetTag("signature.uuid", signature.UUID)

	if signature.GroupUUID != "" {
		span.SetTag("signature.group.uuid", signature.UUID)
	}

	if signature.ChordCallback != nil {
		span.SetTag("signature.chord.callback.uuid", signature.ChordCallback.UUID)
		span.SetTag("signature.chord.callback.name", signature.ChordCallback.Name)
	}
}

// AnnotateSpanWithChainInfo ...
func AnnotateSpanWithChainInfo(span opentracing.Span, chain *tasks.Chain) {
	// tag the span with some info about the chain
	span.SetTag("chain.tasks.length", len(chain.Tasks))

	// inject the tracing span into the tasks signature headers
	for _, signature := range chain.Tasks {
		signature.Headers = HeadersWithSpan(signature.Headers, span)
	}
}

// AnnotateSpanWithGroupInfo ...
func AnnotateSpanWithGroupInfo(span opentracing.Span, group *tasks.Group, sendConcurrency int) {
	// tag the span with some info about the group
	span.SetTag("group.uuid", group.GroupUUID)
	span.SetTag("group.tasks.length", len(group.Tasks))
	span.SetTag("group.concurrency", sendConcurrency)

	// encode the task uuids to json, if that fails just dump it in
	if taskUUIDs, err := json.Marshal(group.GetUUIDs()); err == nil {
		span.SetTag("group.tasks", string(taskUUIDs))
	} else {
		span.SetTag("group.tasks", group.GetUUIDs())
	}

	// inject the tracing span into the tasks signature headers
	for _, signature := range group.Tasks {
		signature.Headers = HeadersWithSpan(signature.Headers, span)
	}
}

// AnnotateSpanWithChordInfo ...
func AnnotateSpanWithChordInfo(span opentracing.Span, chord *tasks.Chord, sendConcurrency int) {
	// tag the span with chord specific info
	span.SetTag("chord.callback.uuid", chord.Callback.UUID)

	// inject the tracing span into the callback signature
	chord.Callback.Headers = HeadersWithSpan(chord.Callback.Headers, span)

	// tag the span for the group part of the chord
	AnnotateSpanWithGroupInfo(span, chord.Group, sendConcurrency)
}
