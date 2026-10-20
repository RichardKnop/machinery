package machinery

import (
	"context"
	"fmt"
	"github.com/RichardKnop/machinery/v2/backends/eager"
	"github.com/RichardKnop/machinery/v2/backends/result"
	"github.com/RichardKnop/machinery/v2/brokers/iface"
	"github.com/RichardKnop/machinery/v2/tasks"
	"testing"
)

type chainSendRecorder struct {
	iface.Broker
	published []*tasks.Signature
}

func (b *chainSendRecorder) Publish(_ context.Context, s *tasks.Signature) error {
	b.published = append(b.published, s)
	return nil
}

func TestSendChainTaskCounts(t *testing.T) {
	for _, withContext := range []bool{false, true} {
		for _, count := range []int{0, 1, 2} {
			t.Run(fmt.Sprintf("context=%v/tasks=%d", withContext, count), func(t *testing.T) {
				signatures := make([]*tasks.Signature, count)
				for i := range signatures {
					signatures[i] = &tasks.Signature{Name: "registered"}
				}
				chain, err := tasks.NewChain(signatures...)
				if err != nil {
					t.Fatal(err)
				}
				broker := &chainSendRecorder{}
				backend := eager.New()
				server := &Server{broker: broker, backend: backend}
				defer func() {
					if p := recover(); p != nil {
						t.Errorf("sending chain panicked: %v", p)
					}
				}()
				var handle *result.ChainAsyncResult
				if withContext {
					handle, err = server.SendChainWithContext(context.Background(), chain)
				} else {
					handle, err = server.SendChain(chain)
				}
				if count == 0 {
					if err == nil || handle != nil || len(broker.published) != 0 {
						t.Fatalf("handle=%v error=%v published=%d; want error without publication", handle, err, len(broker.published))
					}
					return
				}
				if err != nil || handle == nil || len(broker.published) != 1 || broker.published[0] != chain.Tasks[0] {
					t.Fatalf("handle=%v error=%v published=%v", handle, err, broker.published)
				}
				state, err := backend.GetState(chain.Tasks[0].UUID)
				if err != nil || state.State != tasks.StatePending {
					t.Fatalf("state=%v error=%v", state, err)
				}
				if count == 2 && (len(chain.Tasks[0].OnSuccess) != 1 || chain.Tasks[0].OnSuccess[0] != chain.Tasks[1]) {
					t.Fatal("chain continuation changed")
				}
			})
		}
	}
}
