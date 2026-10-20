package machinery

import (
	"context"
	"strings"
	"sync"
	"testing"

	eagerbackend "github.com/RichardKnop/machinery/v2/backends/eager"
	eagerbroker "github.com/RichardKnop/machinery/v2/brokers/eager"
	"github.com/RichardKnop/machinery/v2/config"
	eagerlock "github.com/RichardKnop/machinery/v2/locks/eager"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
)

func TestSendChainWithContextTrace(t *testing.T) {
	for _, parent := range []bool{false, true} {
		name := "background"
		if parent {
			name = "caller span"
		}
		t.Run(name, func(t *testing.T) {
			previous := opentracing.GlobalTracer()
			tracer := mocktracer.New()
			opentracing.SetGlobalTracer(tracer)
			defer opentracing.SetGlobalTracer(previous)
			ctx := context.Background()
			wantTrace := 0
			if parent {
				caller := tracer.StartSpan("caller")
				defer caller.Finish()
				ctx = opentracing.ContextWithSpan(ctx, caller)
				wantTrace = caller.Context().(mocktracer.MockSpanContext).TraceID
			}
			broker := eagerbroker.New()
			backend := eagerbackend.New()
			// No scheduler is needed for this synchronous, in-memory workflow.
			server := &Server{config: &config.Config{}, registeredTasks: new(sync.Map), broker: broker, backend: backend, lock: eagerlock.New()}
			broker.(eagerbroker.Mode).AssignWorker(server.NewWorker("chain-trace", 0))
			var executed []string
			var traces []int
			for _, taskName := range []string{"first", "second"} {
				taskName := taskName
				err := server.RegisterTask(taskName, func(ctx context.Context) error {
					span := opentracing.SpanFromContext(ctx)
					executed = append(executed, taskName)
					traces = append(traces, span.Context().(mocktracer.MockSpanContext).TraceID)
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			chain, err := tasks.NewChain(&tasks.Signature{Name: "first"}, &tasks.Signature{Name: "second"})
			if err != nil {
				t.Fatal(err)
			}
			result, err := server.SendChainWithContext(ctx, chain)
			if err != nil || result == nil {
				t.Fatalf("SendChainWithContext = %v, %v", result, err)
			}
			for _, span := range tracer.FinishedSpans() {
				if span.OperationName == "SendChain" {
					chainTrace := span.Context().(mocktracer.MockSpanContext).TraceID
					if wantTrace == 0 {
						wantTrace = chainTrace
					}
					if chainTrace != wantTrace {
						t.Errorf("chain trace = %d; want %d", chainTrace, wantTrace)
					}
				}
			}
			if len(executed) != 2 || executed[0] != "first" || executed[1] != "second" {
				t.Fatalf("executed = %v", executed)
			}
			for i, trace := range traces {
				if trace != wantTrace || wantTrace == 0 {
					t.Errorf("task %s trace = %d; want chain trace %d", executed[i], trace, wantTrace)
				}
			}
			for _, signature := range chain.Tasks {
				state, err := backend.GetState(signature.UUID)
				if err != nil || state == nil || !state.IsSuccess() {
					t.Errorf("task %s state = %v, %v", signature.Name, state, err)
				}
			}
		})
	}
}

func TestSendChainWithContextRequiresBackend(t *testing.T) {
	chain, err := tasks.NewChain(&tasks.Signature{Name: "not-sent"})
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{}
	result, err := server.SendChainWithContext(context.Background(), chain)
	if result != nil || err == nil || !strings.Contains(err.Error(), "Result backend required") {
		t.Fatalf("missing backend = %v, %v", result, err)
	}
}
