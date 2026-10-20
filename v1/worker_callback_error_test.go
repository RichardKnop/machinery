package machinery

import (
	"context"
	"errors"
	"fmt"
	"github.com/RichardKnop/machinery/v1/backends/eager"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"strings"
	"sync"
	"testing"
)

type callbackPublicationBroker struct {
	iface.Broker
	err       error
	published []*tasks.Signature
}

func (b *callbackPublicationBroker) Publish(_ context.Context, s *tasks.Signature) error {
	b.published = append(b.published, s)
	return b.err
}

func TestWorkerCallbackPublicationErrors(t *testing.T) {
	for _, taskFails := range []bool{false, true} {
		for _, publishFails := range []bool{false, true} {
			t.Run(fmt.Sprintf("taskFailure=%v/publishFailure=%v", taskFails, publishFails), func(t *testing.T) {
				backend := eager.New()
				broker := &callbackPublicationBroker{}
				if publishFails {
					broker.err = errors.New("callback transport unavailable")
				}
				server := &Server{config: &config.Config{}, registeredTasks: new(sync.Map), backend: backend, broker: broker}
				server.registeredTasks.Store("main", func() (int, error) {
					if taskFails {
						return 0, errors.New("task failed")
					}
					return 42, nil
				})
				signature := &tasks.Signature{UUID: "main-task", Name: "main"}
				callback := &tasks.Signature{Name: "callback"}
				wantState := tasks.StateSuccess
				if taskFails {
					signature.OnError = []*tasks.Signature{callback}
					wantState = tasks.StateFailure
				} else {
					signature.OnSuccess = []*tasks.Signature{callback}
				}
				worker := &Worker{server: server}
				err := worker.Process(signature)
				if publishFails {
					if err == nil || !strings.Contains(err.Error(), "callback transport unavailable") {
						t.Errorf("Process error=%v; want callback publication error", err)
					}
				} else if err != nil {
					t.Errorf("Process error=%v; want nil", err)
				}
				if len(broker.published) != 1 || broker.published[0] != callback {
					t.Fatalf("published=%v; want callback", broker.published)
				}
				state, stateErr := backend.GetState(signature.UUID)
				if stateErr != nil || state.State != wantState {
					t.Fatalf("state=%v error=%v; want %s", state, stateErr, wantState)
				}
				wantArg := "42"
				if taskFails {
					wantArg = "task failed"
				}
				if len(callback.Args) != 1 || fmt.Sprint(callback.Args[0].Value) != wantArg {
					t.Fatalf("callback arguments=%v; want %s", callback.Args, wantArg)
				}
			})
		}
	}
}
