package kafka

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/stretchr/testify/assert"
)

type testProcessor struct {
	handled map[string]bool
}

func (t testProcessor) Process(signature *tasks.Signature) error {
	fmt.Println("process")
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
	broker := New(&config.Config{
		Broker: "10.70.2.80:9092",
		Kafka: &config.KafkaConfig{
			Topic:             "machinery_topic",
			DelayedTasksTopic: "machinery_delayed_topic",
			ConsumerGroupId:   "machinery_consumers",
		},
	})

	broker.SetRegisteredTaskNames([]string{"test1", "test2"})

	testProcessor := testProcessor{handled: map[string]bool{}}

	test1 := &tasks.Signature{Name: "test1"}
	delayTime := time.Now().Add(time.Second)
	test2 := &tasks.Signature{Name: "test2", ETA: &delayTime}

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		assert.NoError(t, broker.Publish(context.Background(), test1))
		assert.NoError(t, broker.Publish(context.Background(), test2))
		time.Sleep(time.Second * 10)
		broker.StopConsuming()
		wg.Done()
	}()

	_, err := broker.StartConsuming("worker", 2, testProcessor)
	assert.NoError(t, err)

	wg.Wait()
	assert.True(t, testProcessor.handled["test1"])
	assert.True(t, testProcessor.handled["test2"])
}
