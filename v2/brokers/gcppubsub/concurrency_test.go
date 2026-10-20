package gcppubsub

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/tasks"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

type concurrencyProcessor struct {
	entered chan struct{}
	release chan struct{}
	done    chan struct{}
	active  int32
	maximum int32
}

func (p *concurrencyProcessor) CustomQueue() string     { return "" }
func (p *concurrencyProcessor) PreConsumeHandler() bool { return true }
func (p *concurrencyProcessor) Process(*tasks.Signature) error {
	n := atomic.AddInt32(&p.active, 1)
	for old := atomic.LoadInt32(&p.maximum); n > old; old = atomic.LoadInt32(&p.maximum) {
		if atomic.CompareAndSwapInt32(&p.maximum, old, n) {
			break
		}
	}
	p.entered <- struct{}{}
	<-p.release
	atomic.AddInt32(&p.active, -1)
	p.done <- struct{}{}
	return nil
}

func TestStartConsumingConcurrency(t *testing.T) {
	for _, limit := range []int{1, 2} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			srv := pstest.NewServer()
			defer srv.Close()
			conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			client, err := pubsub.NewClient(ctx, "concurrency-test", option.WithGRPCConn(conn))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			topic, err := client.CreateTopic(ctx, "tasks")
			if err != nil {
				t.Fatal(err)
			}
			defer topic.Stop()
			_, err = client.CreateSubscription(ctx, "worker", pubsub.SubscriptionConfig{Topic: topic})
			if err != nil {
				t.Fatal(err)
			}
			broker, err := New(&config.Config{DefaultQueue: "tasks", GCPPubSub: &config.GCPPubSubConfig{Client: client}}, "concurrency-test", "worker")
			if err != nil {
				t.Fatal(err)
			}
			broker.SetRegisteredTaskNames([]string{"registered"})
			p := &concurrencyProcessor{entered: make(chan struct{}, 8), release: make(chan struct{}), done: make(chan struct{}, 8)}
			stopped := make(chan error, 1)
			go func() { _, err := broker.StartConsuming("worker", limit, p); stopped <- err }()
			defer func() {
				broker.StopConsuming()
				select {
				case err := <-stopped:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Error("consumer did not stop")
				}
			}()
			// Release blocked callbacks before stopping the consumer, including on a failed assertion.
			defer close(p.release)
			for i := 0; i < 8; i++ {
				srv.Publish(topic.String(), []byte(`{"Name":"registered"}`), nil)
			}
			for i := 0; i < limit; i++ {
				select {
				case <-p.entered:
				case <-ctx.Done():
					t.Fatal("consumer did not reach configured concurrency")
				}
			}
			select {
			case <-p.entered:
				t.Errorf("more than %d tasks entered before any completed", limit)
			case <-time.After(300 * time.Millisecond):
			}
			if got := atomic.LoadInt32(&p.maximum); got > int32(limit) {
				t.Errorf("maximum active tasks=%d; limit=%d", got, limit)
			}
		})
	}
}
