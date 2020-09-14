package machinery

import (
	"context"

	"github.com/opentracing/opentracing-go"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/RichardKnop/machinery/v1/tracing"
)

var (
	// These ensure opentracing is automatically called to preserve backwards-compatible behavior
	// TODO: deprecate, or expose
	defaultPreTaskHandler         = OpenTracingPreTaskHandler
	defaultPostTaskHandler        = OpenTracingPostTaskHandler
	defaultPrePublishHandler      = OpenTracingPrePublishTaskHandler
	defaultPreGroupPublishHandler = OpenTracingPreGroupPublishTaskHandler
	defaultPreChordPublishHandler = OpenTracingPreChordPublishTaskHandler
	defaultPreChainPublishHandler = OpenTracingPreChainPublishTaskHandler
)

// finishFun is used by the prepublish handlers on the server
// to ensure a function that returns nil doesn't panic
// is this overkill for panics?
func finishFunc(funcs ...func()) func() {
	return func() {
		for _, f := range funcs {
			f := f // pin
			if f != nil {
				f()
			}
		}
	}
}

// OpenTracingPreTaskHandler starts an opentracing span based on incoming headers
func OpenTracingPreTaskHandler(ctx context.Context, signature *tasks.Signature) context.Context {
	// try to extract trace span from headers and add it to the function context
	// so it can be used inside the function if it has context.Context as the first
	// argument. Start a new span if it isn't found.
	taskSpan := tracing.StartSpanFromHeaders(signature.Headers, signature.Name)
	tracing.AnnotateSpanWithSignatureInfo(taskSpan, signature)
	return opentracing.ContextWithSpan(ctx, taskSpan)
}

// OpenTracingPostTaskHandler ends an opentracing span based on the passed in context
func OpenTracingPostTaskHandler(ctx context.Context, _ *tasks.Signature) {
	taskSpan := opentracing.SpanFromContext(ctx)
	if taskSpan != nil {
		taskSpan.Finish()
	}
}

// OpenTracingPrePublishTaskHandler propagates opentracing information in outgoing task header information
func OpenTracingPrePublishTaskHandler(ctx context.Context, signature *tasks.Signature) func() {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendTask", tracing.ProducerOption(), tracing.MachineryTag)

	// tag the span with some info about the signature
	signature.Headers = tracing.HeadersWithSpan(signature.Headers, span)

	return span.Finish
}

// OpenTracingPreGroupPublishTaskHandler propagates opentracing information in outgoing task header information
func OpenTracingPreGroupPublishTaskHandler(ctx context.Context, group *tasks.Group, sendConcurrency int) func() {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendGroup", tracing.ProducerOption(), tracing.MachineryTag, tracing.WorkflowGroupTag)

	tracing.AnnotateSpanWithGroupInfo(span, group, sendConcurrency)

	return span.Finish
}

// OpenTracingPreChainPublishTaskHandler propagates opentracing information in outgoing task header information
func OpenTracingPreChainPublishTaskHandler(ctx context.Context, chain *tasks.Chain) func() {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendChain", tracing.ProducerOption(), tracing.MachineryTag, tracing.WorkflowChainTag)
	tracing.AnnotateSpanWithChainInfo(span, chain)
	return span.Finish
}

// OpenTracingPreChordPublishTaskHandler propagates opentracing information in outgoing task header information
func OpenTracingPreChordPublishTaskHandler(ctx context.Context, chord *tasks.Chord, sendConcurrency int) func() {
	span, _ := opentracing.StartSpanFromContext(ctx, "SendChord", tracing.ProducerOption(), tracing.MachineryTag, tracing.WorkflowChordTag)
	tracing.AnnotateSpanWithChordInfo(span, chord, sendConcurrency)

	return span.Finish
}
