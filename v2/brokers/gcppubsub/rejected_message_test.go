package gcppubsub

import (
	"errors"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/RichardKnop/machinery/v2/brokers/iface"
	"github.com/RichardKnop/machinery/v2/common"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/tasks"
)

type rejectedMessageProcessor struct {
	iface.TaskProcessor
	calls int
	err   error
}

func (p *rejectedMessageProcessor) Process(s *tasks.Signature) error {
	p.calls++
	return p.err
}

func TestConsumeOneRejectedMessages(t *testing.T) {
	for _, tc := range []struct {
		name, data   string
		wantCalls    int
		processError bool
	}{
		{name: "empty"},
		{name: "invalid JSON", data: `{"Name":"registered","Args":}`},
		{name: "unregistered", data: `{"Name":"unknown"}`},
		{name: "registered", data: `{"Name":"registered"}`, wantCalls: 1},
		{name: "processor error", data: `{"Name":"registered"}`, wantCalls: 1, processError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			broker := &Broker{Broker: common.NewBroker(&config.Config{})}
			broker.SetRegisteredTaskNames([]string{"registered"})
			processor := &rejectedMessageProcessor{}
			if tc.processError {
				processor.err = errors.New("task failed")
			}
			// An ordinary SDK message is sufficient to observe task dispatch. It has
			// no acknowledgement handler, so this test makes no service-ack claim.
			broker.consumeOne(&pubsub.Message{Data: []byte(tc.data)}, processor)
			if processor.calls != tc.wantCalls {
				t.Fatalf("Process calls=%d; want %d", processor.calls, tc.wantCalls)
			}
		})
	}
}
