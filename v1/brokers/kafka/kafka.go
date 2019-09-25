package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/Shopify/sarama"
)

// Broker represents a Kafka broker.
type Broker struct {
	common.Broker
	// tasksWG is used to wait on tasks processed. If consumer
	// closes it waits for waitgroup to finish before closing.
	tasksWG sync.WaitGroup
	// consumerWG is used to wait on consumer to finish closing
	// before fully closing.
	consumerWG sync.WaitGroup
	// Kafka consumer group.
	consumer sarama.ConsumerGroup
	// Kafka producer.
	producer sarama.SyncProducer
	// Queue where consumed tasks are pushed.
	tskQueue chan []byte
	// Queue where errors while processing tasks are pushed.
	errQueue chan error
}

const (
	// defaultClientID is Kafka consumer client id.
	defaultClientID   = "machinery"
	compressionGZIP   = "gzip"
	compressionLZ4    = "lz4"
	compressionSnappy = "snappy"
	compressionZSTD   = "zstd"
)

// New creates new Kafka broker instance.
func New(cnf *config.Config) iface.Broker {
	b := &Broker{Broker: common.NewBroker(cnf)}
	// Create Kafka consumer group and attach it.
	b.consumer = newConsumerGroup(cnf.Kafka)
	// Create Kafka sync producer and attach it.
	p, err := newProducer(cnf.Kafka)
	if err != nil {
		log.FATAL.Fatalf("error creating kafka producer: %v", err)
	}
	b.producer = p
	return b
}

// newProducer creates a new Kafka sync produce.
func newProducer(cnf *config.KafkaConfig) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	// Set compresion format and level.
	if cnf.Compression != "" {
		switch cnf.Compression {
		case compressionGZIP:
			config.Producer.Compression = sarama.CompressionGZIP
		case compressionLZ4:
			config.Producer.Compression = sarama.CompressionLZ4
		case compressionSnappy:
			config.Producer.Compression = sarama.CompressionSnappy
		case compressionZSTD:
			config.Producer.Compression = sarama.CompressionZSTD
		default:
			return nil, fmt.Errorf("unsupported compression format: %s. Supported formats are: %s, %s, %s, %s",
				cnf.Compression, compressionGZIP, compressionLZ4, compressionSnappy, compressionZSTD)
		}
		if cnf.CompressionLevel != nil {
			config.Producer.CompressionLevel = *cnf.CompressionLevel
		}
	}
	if cnf.MessageSize != 0 {
		config.Producer.MaxMessageBytes = cnf.MessageSize
	}
	return sarama.NewSyncProducer(cnf.Addrs, config)
}

// newConsumerGroup creates a new Kafka consumer group.
func newConsumerGroup(cnf *config.KafkaConfig) sarama.ConsumerGroup {
	config := sarama.NewConfig()
	// If client id is not set then set default client id.
	if cnf.ClientID == "" {
		config.ClientID = defaultClientID
	} else {
		config.ClientID = cnf.ClientID
	}
	// Set Kafka version.
	config.Version = sarama.V2_1_0_0
	// Errors will be pushed to a channel instead of ignoring.
	config.Consumer.Return.Errors = true
	// Its always set to oldest because if group name is different then
	// it should start consuming from the beginning.
	if cnf.OffsetNewest {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	// Set max consume size.
	if cnf.MessageSize != 0 {
		config.Consumer.Fetch.Max = int32(cnf.MessageSize)
	}
	// Create a new consumer group.
	cGroup, err := sarama.NewConsumerGroup(cnf.Addrs, cnf.Group, config)
	if err != nil {
		log.FATAL.Fatalf("error creating consumer group: %v", err)
	}
	// Run go routine which logs all kafka consumer errors.
	go func() {
		for err := range cGroup.Errors() {
			log.ERROR.Printf("kafka consumer error: %v", err)
		}
	}()
	return cGroup
}

// Setup runs while setting up kafka consumer.
func (b *Broker) Setup(cgs sarama.ConsumerGroupSession) error { return nil }

// Cleanup cleans up after Kafka consumer.
func (b *Broker) Cleanup(cgs sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim consumes the message sent to a topic and marks it as claimed.
// This method is internally called by Sarama when consumer starts consuming.
func (b *Broker) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Read message pushed to topic.
	for {
		select {
		// A way to stop this goroutine from b.StopConsuming
		case <-b.GetStopChan():
			// Close tasks queue so that new tasks wont be able to push.
			close(b.tskQueue)
			return nil
		case msg := <-claim.Messages():
			// Add tasks wait group. Once message is processed its marked as done.
			// This way when consumer is closing we wait for this wait group to finish before exiting.
			b.tasksWG.Add(1)
			// Add a message to task queue which is consumed elsewhere.
			b.tskQueue <- msg.Value
			// Mark the message immediately after user is inserted to db.
			// If process is stopped while inserting/updating user then on
			// restart its consumed again.
			sess.MarkMessage(msg, "")
		}
	}
}

// tskWorker consumes from tasks queue and runs `taskProcessor.Process` call which
// executes the actual task. If task execution results in error then send to errors
// queue which is logged separately in a go routine.
func (b *Broker) tskWorker(id int, tskPr iface.TaskProcessor) {
	for msg := range b.tskQueue {
		log.DEBUG.Printf("processing task - worker_id: %v, msg: %s", id, string(msg))
		// Process the task.
		if err := b.processTask(msg, tskPr); err != nil {
			b.errQueue <- err
		}
		// Mark task as done in wait group once task is processed.
		b.tasksWG.Done()
	}
}

// processTask creates a machinery task from bytes received from broker and
// calls `taskProcessor.Process` which run the task.
func (b *Broker) processTask(msg []byte, tskPr iface.TaskProcessor) error {
	// Create a machinery task from received byte messages.
	tsk := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(msg))
	decoder.UseNumber()
	if err := decoder.Decode(tsk); err != nil {
		return errs.NewErrCouldNotUnmarshaTaskSignature(msg, err)
	}
	// TODO: If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(tsk.Name) {
	}
	return tskPr.Process(tsk)
}

// GetPendingTasks returns a slice of task.Signatures waiting in the queue.
// Since Kafka doesn't support this its not implemented.
func (b *Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	return nil, errors.New("Not implemented")
}

// StartConsuming starts the Kafka consumer and waits for messages.
// This is also responsible for initializing task queues and creating
// worker pool which consumes the message received from Kafka.
func (b *Broker) StartConsuming(cTag string, con int, tskPr iface.TaskProcessor) (bool, error) {
	// If concurrency is not defined then its set to 1.
	// At a time single message is processed.
	if con < 1 {
		con = 1
	}
	// Call brokerfactory which initialized channels for stop and retry consumer.
	b.Broker.StartConsuming(cTag, con, tskPr)
	ctx := context.Background()
	// Add consumer to wait group.
	b.consumerWG.Add(1)
	defer b.consumerWG.Done()
	// Initialize task queue.
	b.tskQueue = make(chan []byte, con)
	// Initialize error queue.
	b.errQueue = make(chan error, con*2)
	// Initialize worker pools.
	for p := 0; p <= con; p++ {
		go b.tskWorker(p, tskPr)
	}
	// Log all the errors while processing tasks.
	go func() {
		for err := range b.errQueue {
			log.ERROR.Printf("task processing failed: %v", err)
		}
	}()
	log.INFO.Printf("Waiting for tasks from topics: %s", strings.Join(b.getTopics(tskPr), ","))
	if err := b.consumer.Consume(ctx, b.getTopics(tskPr), b); err != nil {
		return b.GetRetry(), err
	}
	return b.GetRetry(), nil
}

// StopConsuming quits the loop.
func (b *Broker) StopConsuming() {
	b.Broker.StopConsuming()
	// Waiting for consumer to be stopped.
	b.consumerWG.Wait()
	// Waiting for any tasks being processed to finish.
	b.tasksWG.Wait()
	// Close the error channel when all tasks are processed.
	close(b.errQueue)
}

// Publish places a new message on the default queue.
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) error {
	b.Broker.AdjustRoutingKey(signature)
	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %v", err)
	}
	pMsg := &sarama.ProducerMessage{
		Topic:     signature.RoutingKey,
		Partition: -1,
		Value:     sarama.StringEncoder(msg),
	}
	_, offset, err := b.producer.SendMessage(pMsg)
	if err != nil {
		return fmt.Errorf("error sending message: %v", err)
	}
	log.DEBUG.Printf("sent message - topic: %s, offset: %v, msg: %s", signature.RoutingKey, offset, string(msg))
	return nil
}

// getTopics gets current kafka topics.
func (b *Broker) getTopics(tskPr iface.TaskProcessor) []string {
	if tskPr.CustomQueue() != "" {
		return []string{tskPr.CustomQueue()}
	}
	return []string{b.GetConfig().DefaultQueue}
}
