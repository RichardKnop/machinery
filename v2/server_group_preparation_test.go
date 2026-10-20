package machinery

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v2/backends/eager"
	backendiface "github.com/RichardKnop/machinery/v2/backends/iface"
	brokeriface "github.com/RichardKnop/machinery/v2/brokers/iface"
	"github.com/RichardKnop/machinery/v2/tasks"
)

// Inject failures at the persistence interface while retaining the real eager
// backend for all successful operations.
type groupPreparationBackend struct {
	backendiface.Backend
	failure  error
	failInit bool
	pending  int
}

func (b *groupPreparationBackend) InitGroup(id string, ids []string) error {
	if b.failInit {
		return b.failure
	}
	return b.Backend.InitGroup(id, ids)
}
func (b *groupPreparationBackend) SetStatePending(s *tasks.Signature) error {
	b.pending++
	if b.pending == 2 && b.failure != nil && !b.failInit {
		return b.failure
	}
	return b.Backend.SetStatePending(s)
}

// Observe the publication boundary; no external broker or service is involved.
type groupPreparationBroker struct {
	brokeriface.Broker
	published int32
}

func (b *groupPreparationBroker) Publish(context.Context, *tasks.Signature) error {
	atomic.AddInt32(&b.published, 1)
	return nil
}

func TestSendGroupPreparationErrors(t *testing.T) {
	for _, stage := range []string{"group", "second task", "success"} {
		t.Run(stage, func(t *testing.T) {
			failure := errors.New("persistence unavailable")
			backend := &groupPreparationBackend{Backend: eager.New(), failure: failure, failInit: stage == "group"}
			if stage == "success" {
				backend.failure = nil
			}
			broker := &groupPreparationBroker{}
			server := &Server{backend: backend, broker: broker}
			group, err := tasks.NewGroup(&tasks.Signature{Name: "first"}, &tasks.Signature{Name: "second"}, &tasks.Signature{Name: "third"})
			if err != nil {
				t.Fatal(err)
			}
			results, err := server.SendGroupWithContext(context.Background(), group, 1)
			if stage == "success" {
				if err != nil || len(results) != 3 {
					t.Fatalf("results=%v error=%v", results, err)
				}
				if got := atomic.LoadInt32(&broker.published); got != 3 {
					t.Fatalf("published=%d; want 3", got)
				}
				for _, s := range group.Tasks {
					state, e := backend.GetState(s.UUID)
					if e != nil || state.State != tasks.StatePending {
						t.Fatalf("pending state=%v error=%v", state, e)
					}
				}
				return
			}
			if !errors.Is(err, failure) {
				t.Errorf("error=%v; want original persistence error", err)
			}
			if results != nil {
				t.Error("preparation failure returned result handles")
			}
			// The old implementation returns while publication goroutines are active.
			// Allow their effects to become visible before checking that none ran.
			time.Sleep(20 * time.Millisecond)
			if got := atomic.LoadInt32(&broker.published); got != 0 {
				t.Errorf("published=%d after preparation failure; want 0", got)
			}
			wantPending := 2
			if stage == "group" {
				wantPending = 0
			}
			if backend.pending != wantPending {
				t.Errorf("pending calls=%d; want %d", backend.pending, wantPending)
			}
		})
	}
}
