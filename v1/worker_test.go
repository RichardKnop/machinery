package machinery_test

import (
	"context"
	"testing"

	eagerbackend "github.com/RichardKnop/machinery/v1/backends/eager"
	"github.com/RichardKnop/machinery/v1/brokers/eager"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/suite"

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

type workerSuite struct {
	worker *machinery.Worker
	srv    *machinery.Server
	suite.Suite
}

func TestWorkerContextHandlers(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(workerSuite))
}

func (w *workerSuite) SetupSuite() {
	w.srv = machinery.NewServerWithBrokerBackend(nil, eager.New(), eagerbackend.New())
	w.worker = w.srv.NewWorker("", 1)
}

func (w *workerSuite) TestPreTaskHandler_Default() {
	var ctx context.Context
	_ = w.srv.RegisterTask("abc", func(c context.Context) error {
		ctx = c
		return nil
	})
	sig := &tasks.Signature{
		Name: "abc",
	}
	err := w.worker.Process(sig)
	w.NoError(err)
	w.NotNil(ctx)
	w.NotNil(opentracing.SpanFromContext(ctx), "opentracing span should have been passed to the context")
}

func (w *workerSuite) TestPreTaskHandler_Nil() {
	var ctx context.Context

	w.worker.SetPreTaskContextHandler(nil)

	_ = w.srv.RegisterTask("abc", func(c context.Context) error {
		ctx = c
		return nil
	})

	sig := &tasks.Signature{
		Name: "abc",
	}

	w.Panics(func() {
		_ = w.worker.Process(sig)
	})
	w.Nil(ctx)
}

func (w *workerSuite) TestPreTaskHandler_Custom() {
	var ctx context.Context

	w.worker.SetPreTaskContextHandler(func(ctx context.Context, _ *tasks.Signature) context.Context {
		return context.WithValue(ctx, "a", "b")
	})

	_ = w.srv.RegisterTask("abc", func(c context.Context) error {
		ctx = c
		return nil
	})

	sig := &tasks.Signature{
		Name: "abc",
	}
	err := w.worker.Process(sig)
	w.NoError(err)
	w.NotNil(ctx)
	w.NotNil(opentracing.SpanFromContext(ctx), "opentracing span should have been passed to the context")
	w.Equal("b", ctx.Value("a"), "worker pre task function should have set a value in the task context")
}
