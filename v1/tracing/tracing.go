package tracing

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/RichardKnop/machinery/v1/tasks"

	"go.opencensus.io/trace"
)

// opencensus attributes
var (
	MachineryAttribute     = trace.StringAttribute("component", "machinery")
	WorkflowGroupAttribute = trace.StringAttribute("machinery.workflow", "group")
	WorkflowChordAttribute = trace.StringAttribute("machinery.workflow", "chord")
	WorkflowChainAttribute = trace.StringAttribute("machinery.workflow", "chain")

	traceParentHeader = `traceparent`
)

// StartSpanFromHeaders will extract a span from the signature headers
// and start a new span with the given operation name.
func StartSpanFromHeaders(ctx context.Context, headers tasks.Headers, operationName string) (context.Context, *trace.Span) {
	spanContext := trace.SpanContext{}
	if h, ok := headers[traceParentHeader]; ok {
		if value, ok := h.(string); ok {
			spanContext = spanContextFromHeader(value)
		}
	}

	return trace.StartSpanWithRemoteParent(ctx, operationName, spanContext)
}

// spanContextFromHeader reads the traceparent based on w3c's standard
// https://w3c.github.io/trace-context/#traceparent-field
func spanContextFromHeader(h string) (sc trace.SpanContext) {
	splits := strings.Split(h, `-`)

	if len(splits) != 4 || splits[0] != "00" {
		return trace.SpanContext{}
	}

	if len(splits[1]) != 32 {
		return trace.SpanContext{}
	}

	buf, err := hex.DecodeString(splits[1])
	if err != nil {
		return trace.SpanContext{}
	}
	copy(sc.TraceID[:], buf)

	if len(splits[2]) != 16 {
		return trace.SpanContext{}
	}

	buf, err = hex.DecodeString(splits[2])
	if err != nil {
		return trace.SpanContext{}
	}
	copy(sc.SpanID[:], buf)

	if len(splits[3]) != 2 {
		return trace.SpanContext{}
	}

	o, err := strconv.ParseUint(splits[3], 16, 32)

	if err != nil {
		return trace.SpanContext{}
	}

	sc.TraceOptions = trace.TraceOptions(o)

	if sc.TraceID == [16]byte{} || sc.SpanID == [8]byte{} {
		return trace.SpanContext{}
	}

	return sc
}

// HeadersWithSpan will inject a span into the signature headers
func HeadersWithSpan(headers tasks.Headers, span *trace.Span) tasks.Headers {
	// check if the headers aren't nil
	if headers == nil {
		headers = make(tasks.Headers)
	}

	headers[traceParentHeader] = headerFromSpanContext(span.SpanContext())

	return headers
}

func headerFromSpanContext(sc trace.SpanContext) string {
	return fmt.Sprintf("%x-%x-%x-%x",
		[]byte{0x00},
		sc.TraceID[:],
		sc.SpanID[:],
		[]byte{byte(sc.TraceOptions)})
}

// AnnotateSpanWithSignatureInfo ...
func AnnotateSpanWithSignatureInfo(span *trace.Span, signature *tasks.Signature) {
	span.AddAttributes(MachineryAttribute)
	// tag the span with some info about the signature
	span.AddAttributes(trace.StringAttribute("signature.name", signature.Name))
	span.AddAttributes(trace.StringAttribute("signature.uuid", signature.UUID))

	if signature.GroupUUID != "" {
		span.AddAttributes(trace.StringAttribute("signature.group.uuid", signature.UUID))
	}

	if signature.ChordCallback != nil {
		span.AddAttributes(trace.StringAttribute("signature.chord.callback.uuid", signature.ChordCallback.UUID))
		span.AddAttributes(trace.StringAttribute("signature.chord.callback.name", signature.ChordCallback.Name))
	}
}

// AnnotateSpanWithChainInfo ...
func AnnotateSpanWithChainInfo(span *trace.Span, chain *tasks.Chain) {
	span.AddAttributes(MachineryAttribute)
	span.AddAttributes(WorkflowChainAttribute)

	// tag the span with some info about the chain
	span.AddAttributes(trace.Int64Attribute("chain.tasks.length", int64(len(chain.Tasks))))

	// inject the tracing span into the tasks signature headers
	for _, signature := range chain.Tasks {
		signature.Headers = HeadersWithSpan(signature.Headers, span)
	}
}

// AnnotateSpanWithGroupInfo ...
func AnnotateSpanWithGroupInfo(span *trace.Span, group *tasks.Group, sendConcurrency int) {
	span.AddAttributes(MachineryAttribute)
	span.AddAttributes(WorkflowGroupAttribute)

	// tag the span with some info about the group
	span.AddAttributes(trace.StringAttribute("group.uuid", group.GroupUUID))
	span.AddAttributes(trace.Int64Attribute("group.tasks.length", int64(len(group.Tasks))))
	span.AddAttributes(trace.Int64Attribute("group.concurrency", int64(sendConcurrency)))

	// encode the task uuids to json, if that fails skip it
	if taskUUIDs, err := json.Marshal(group.GetUUIDs()); err == nil {
		span.AddAttributes(trace.StringAttribute("group.tasks", string(taskUUIDs)))
	}

	// inject the tracing span into the tasks signature headers
	for _, signature := range group.Tasks {
		signature.Headers = HeadersWithSpan(signature.Headers, span)
	}
}

// AnnotateSpanWithChordInfo ...
func AnnotateSpanWithChordInfo(span *trace.Span, chord *tasks.Chord, sendConcurrency int) {
	span.AddAttributes(MachineryAttribute)
	span.AddAttributes(WorkflowChordAttribute)

	// tag the span with chord specific info
	span.AddAttributes(trace.StringAttribute("chord.callback.uuid", chord.Callback.UUID))

	// inject the tracing span into the callback signature
	chord.Callback.Headers = HeadersWithSpan(chord.Callback.Headers, span)

	// tag the span for the group part of the chord
	AnnotateSpanWithGroupInfo(span, chord.Group, sendConcurrency)
}
