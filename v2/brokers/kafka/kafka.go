package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/RichardKnop/machinery/v2/brokers/errs"
	"github.com/RichardKnop/machinery/v2/brokers/iface"
	"github.com/RichardKnop/machinery/v2/common"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/log"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/segmentio/kafka-go"
)

type KafkaBroker struct {
	common.Broker
	reader        *kafka.Reader
	writer        *kafka.Writer
	delayedReader *kafka.Reader
	delayedWriter *kafka.Writer

	consumePeriod time.Duration

	consumingWG  sync.WaitGroup // wait group to make sure whole consumption completes
	processingWG sync.WaitGroup // use wait group to make sure task processing completes
	delayedWG    sync.WaitGroup
}

type messageInfo struct {
	message kafka.Message
	reader  *kafka.Reader
}

func New(cnf *config.Config) *KafkaBroker {
	brokers := strings.Split(cnf.Broker, ",")

	readerCfg := func(topic string) kafka.ReaderConfig {
		return kafka.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        cnf.Kafka.ConsumerGroupId,
			CommitInterval: time.Duration(cnf.Kafka.CommitInterval) * time.Millisecond,
		}
	}

	writerCfg := func(topic string) kafka.WriterConfig {
		return kafka.WriterConfig{
			Brokers: brokers,
			Topic:   topic,
		}
	}

	consumePeriod := 500 // default poll period for delayed tasks
	if cnf.Kafka != nil {
		configuredConsumePeriod := cnf.Kafka.DelayedTasksConsumePeriod
		if configuredConsumePeriod > 0 {
			consumePeriod = configuredConsumePeriod
		}
	}

	topic, delayedTasksTopic := cnf.Kafka.Topic, cnf.Kafka.DelayedTasksTopic
	return &KafkaBroker{
		Broker:        common.NewBroker(cnf),
		reader:        kafka.NewReader(readerCfg(topic)),
		writer:        kafka.NewWriter(writerCfg(topic)),
		delayedReader: kafka.NewReader(readerCfg(delayedTasksTopic)),
		delayedWriter: kafka.NewWriter(writerCfg(delayedTasksTopic)),
		consumePeriod: time.Duration(consumePeriod) * time.Millisecond,
	}
}

func (b *KafkaBroker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.consumingWG.Add(1)
	defer b.consumingWG.Done()

	if concurrency < 1 {
		concurrency = runtime.NumCPU() * 2
	}

	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)

	// Channel to which we will push tasks ready for processing by worker
	deliveries := make(chan messageInfo, concurrency)

	errorsChan := make(chan error, 1)

	go func() {
		log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				close(deliveries)
				return
			default:
				ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*30)
				defer cancelFunc()
				m, err := b.reader.ReadMessage(ctx)

				// timeout error, then retry
				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				if err != nil {
					errorsChan <- err
					return
				}
				deliveries <- messageInfo{message: m, reader: b.reader}
			}
		}
	}()

	b.delayedWG.Add(1)
	go func() {
		defer b.delayedWG.Done()
		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				return
			default:
				err := b.processDelayedTask()
				if err != nil {
					errorsChan <- err
					return
				}
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor, errorsChan); err != nil {
		return b.GetRetry(), err
	}

	b.processingWG.Wait()

	return b.GetRetry(), nil
}

func (b *KafkaBroker) consume(deliveries <-chan messageInfo, concurrency int, taskProcessor iface.TaskProcessor, errorsChan chan error) error {
	pool := make(chan struct{}, concurrency)

	// init pool for Worker tasks execution, as many slots as Worker concurrency param
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}

	for {
		select {
		case err := <-errorsChan:
			return err
		case d, open := <-deliveries:
			if !open {
				return nil
			}
			if concurrency > 0 {
				// get execution slot from pool (blocks until one is available)
				<-pool
			}

			b.processingWG.Add(1)

			// Consume the task inside a goroutine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d.reader, d.message, taskProcessor); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give slot back to pool
					pool <- struct{}{}
				}
			}()
		}
	}
}

func (b *KafkaBroker) consumeOne(reader *kafka.Reader, message kafka.Message, taskProcessor iface.TaskProcessor) error {
	defer reader.CommitMessages(context.Background(), message)

	// Unmarshal message body into signature struct
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(message.Value))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return errs.NewErrCouldNotUnmarshalTaskSignature(message.Value, err)
	}

	// If the task is not registered, we nack it and requeue,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		log.INFO.Printf("Task not registered with this worker. Requeing message: %s", message.Value)
		return nil
	}
	log.DEBUG.Printf("Received new message: %s", message.Value)
	err := taskProcessor.Process(signature)
	return err
}

func (b *KafkaBroker) processDelayedTask() error {

	time.Sleep(b.consumePeriod)

	ctx, cancelFunc := context.WithTimeout(context.Background(), b.consumePeriod)
	defer cancelFunc()
	m, err := b.delayedReader.ReadMessage(ctx)

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		return err
	}
	defer b.delayedReader.CommitMessages(context.Background(), m)

	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(m.Value))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return errs.NewErrCouldNotUnmarshalTaskSignature(m.Value, err)
	}

	if err := b.Publish(context.Background(), signature); err != nil {
		return err
	}
	return nil
}

func (b *KafkaBroker) Publish(ctx context.Context, signature *tasks.Signature) error {
	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			err = b.delayedWriter.WriteMessages(ctx, kafka.Message{Value: msg})
			return err
		}
	}

	err = b.writer.WriteMessages(ctx, kafka.Message{Value: msg})
	return err
}

// StopConsuming quits the loop
func (b *KafkaBroker) StopConsuming() {
	b.Broker.StopConsuming()
	// Waiting for the delayed tasks goroutine to have stopped
	b.delayedWG.Wait()
	// Waiting for consumption to finish
	b.consumingWG.Wait()

	b.reader.Close()
	b.delayedReader.Close()

	b.writer.Close()
	b.delayedWriter.Close()
}
