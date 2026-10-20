package redis

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v2/brokers/iface"
	"github.com/RichardKnop/machinery/v2/common"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/tasks"
)

type shutdownErrorProcessor struct {
	iface.TaskProcessor
	err error
}

func (p *shutdownErrorProcessor) Process(*tasks.Signature) error { return p.err }

func TestConsumeStopsProducerOnProcessingError(t *testing.T) {
	b := &Broker{Broker: common.NewBroker(&config.Config{})}
	b.SetRegisteredTaskNames([]string{"registered"})
	failure := errors.New("processing failed")
	deliveries := make(chan []byte)
	stopProducer := make(chan struct{})
	fallback := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(fallback) }) }
	producerDone := make(chan struct{})
	// Follow StartConsuming's producer shutdown protocol without a Redis server.
	// The producer closes deliveries only after receiving the stop signal.
	go func() {
		defer close(producerDone)
		defer close(deliveries)
		deliveries <- []byte(`{"Name":"registered"}`)
		select {
		case <-stopProducer:
		case <-fallback: // test cleanup also releases the original broken implementation
		}
	}()
	result := make(chan error, 1)
	go func() { result <- b.consume(deliveries, 1, &shutdownErrorProcessor{err: failure}, stopProducer) }()
	defer func() {
		release()
		select {
		case <-producerDone:
		case <-time.After(time.Second):
			t.Error("producer did not stop")
		}
	}()
	select {
	case <-stopProducer:
	case <-time.After(time.Second):
		t.Error("consumer did not signal producer shutdown after processing error")
		release()
	}
	select {
	case err := <-result:
		if err != failure {
			t.Fatalf("error=%v; want original processing error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("consumer did not return after producer stopped")
	}
}

func TestConsumeClosedDeliveries(t *testing.T) {
	b := &Broker{Broker: common.NewBroker(&config.Config{})}
	deliveries := make(chan []byte)
	close(deliveries)
	stopProducer := make(chan struct{})
	if err := b.consume(deliveries, 1, &shutdownErrorProcessor{}, stopProducer); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopProducer:
		t.Error("normal producer shutdown was signalled as an error")
	default:
	}
}
