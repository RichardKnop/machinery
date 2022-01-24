package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v2/common"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

// Kafka broker used for test implement MessageReader and MessageWriter
type testKafkaBroker struct {
	msgs chan kafka.Message
}

func (t testKafkaBroker) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	for _, msg := range msgs {
		select {
		case <-ctx.Done():
			return context.DeadlineExceeded
		case t.msgs <- msg:
		}

	}
	return nil
}

func (t testKafkaBroker) ReadMessage(ctx context.Context) (kafka.Message, error) {
	select {
	case <-ctx.Done():
		return kafka.Message{}, context.DeadlineExceeded
	case msg := <-t.msgs:
		return msg, nil
	}
}

func (t testKafkaBroker) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return nil
}

func (t testKafkaBroker) Close() error {
	return nil
}

// Task processor that implement iface.TaskProcessor
type testProcessor struct {
	handled map[string]bool
}

func (t testProcessor) Process(signature *tasks.Signature) error {
	t.handled[signature.Name] = true
	return nil
}

func (t testProcessor) CustomQueue() string {
	return ""
}

func (t testProcessor) PreConsumeHandler() bool {
	return true
}

func TestKafkaBroker(t *testing.T) {
	normalKafka := testKafkaBroker{msgs: make(chan kafka.Message, 10)}
	delayedKafka := testKafkaBroker{msgs: make(chan kafka.Message, 10)}

	broker := &KafkaBroker{
		Broker:         common.NewBroker(&config.Config{}),
		reader:         normalKafka,
		writer:         normalKafka,
		delayedReader:  delayedKafka,
		delayedWriter:  delayedKafka,
		consumePeriod:  time.Millisecond,
		consumeTimeout: time.Second,
	}

	broker.SetRegisteredTaskNames([]string{"test1", "test2"})

	testProcessor := testProcessor{handled: map[string]bool{}}

	test1 := &tasks.Signature{Name: "test1"}
	delayTime := time.Now().Add(time.Millisecond)
	test2 := &tasks.Signature{Name: "test2", ETA: &delayTime}

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		assert.NoError(t, broker.Publish(context.Background(), test1))
		assert.NoError(t, broker.Publish(context.Background(), test2))
		time.Sleep(time.Second)
		broker.StopConsuming()
		wg.Done()
	}()

	_, err := broker.StartConsuming("worker", 2, testProcessor)
	assert.NoError(t, err)

	wg.Wait()

	// Check test1 and test2 be consumed of not
	assert.True(t, testProcessor.handled["test1"])
	assert.True(t, testProcessor.handled["test2"])
}
