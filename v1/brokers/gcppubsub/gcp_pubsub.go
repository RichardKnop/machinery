package gcppubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// Broker represents an Google Cloud Pub/Sub broker
type Broker struct {
	common.Broker

	service          *pubsub.Client
	subscriptionName string
	MaxExtension     time.Duration

	processingWG sync.WaitGroup
}

// New creates new Broker instance
func New(cnf *config.Config, projectID, subscriptionName string) (iface.Broker, error) {
	b := &Broker{Broker: common.NewBroker(cnf)}
	b.subscriptionName = subscriptionName

	ctx := context.Background()

	if cnf.GCPPubSub != nil {
		b.MaxExtension = cnf.GCPPubSub.MaxExtension
	}

	if cnf.GCPPubSub != nil && cnf.GCPPubSub.Client != nil {
		b.service = cnf.GCPPubSub.Client
	} else {
		pubsubClient, err := pubsub.NewClient(ctx, projectID)
		if err != nil {
			return nil, err
		}
		b.service = pubsubClient
		cnf.GCPPubSub = &config.GCPPubSubConfig{
			Client: pubsubClient,
		}
	}

	// Validate topic exists
	defaultQueue := b.GetConfig().DefaultQueue
	topic := b.service.Topic(defaultQueue)
	defer topic.Stop()

	topicExists, err := topic.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !topicExists {
		return nil, fmt.Errorf("topic does not exist, instead got %s", defaultQueue)
	}

	// Validate subscription exists
	sub := b.service.Subscription(b.subscriptionName)

	if b.MaxExtension != 0 {
		sub.ReceiveSettings.MaxExtension = b.MaxExtension
	}

	subscriptionExists, err := sub.Exists(ctx)
	if err != nil {
		return nil, err
	}
	if !subscriptionExists {
		return nil, fmt.Errorf("subscription does not exist, instead got %s", b.subscriptionName)
	}

	return b, nil
}

// StartConsuming enters a loop and waits for incoming messages
func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)
	deliveries := make(chan *pubsub.Message)

	sub := b.service.Subscription(b.subscriptionName)

	if b.MaxExtension != 0 {
		sub.ReceiveSettings.MaxExtension = b.MaxExtension
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				cancel()
				return
			default:
				err := sub.Receive(ctx, func(_ctx context.Context, msg *pubsub.Message) {
					deliveries <- msg
				})

				if err != nil {
					log.ERROR.Printf("Error when receiving messages. Error: %v", err)
					continue
				}
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor); err != nil {
		return b.GetRetry(), err
	}

	return b.GetRetry(), nil
}

// StopConsuming quits the loop
func (b *Broker) StopConsuming() {
	b.Broker.StopConsuming()

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()
}

// Publish places a new message on the default queue
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) error {
	// Adjust routing key (this decides which queue the message will be published to)
	b.AdjustRoutingKey(signature)

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	defaultQueue := b.GetConfig().DefaultQueue
	topic := b.service.Topic(defaultQueue)
	defer topic.Stop()

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			topic.PublishSettings.DelayThreshold = signature.ETA.Sub(now)
		}
	}

	result := topic.Publish(ctx, &pubsub.Message{
		Data: msg,
	})

	id, err := result.Get(ctx)
	if err != nil {
		log.ERROR.Printf("Error when sending a message: %v", err)
		return err
	}

	log.INFO.Printf("Sending a message successfully, server-generated message ID %v", id)
	return nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *Broker) consume(deliveries <-chan *pubsub.Message, concurrency int, taskProcessor iface.TaskProcessor) error {
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	errorsChan := make(chan error)

	for {
		select {
		case err := <-errorsChan:
			return err
		case d := <-deliveries:
			if concurrency > 0 {
				// get worker from pool (blocks until one is available)
				<-pool
			}

			b.processingWG.Add(1)

			// Consume the task inside a gotourine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give worker back to pool
					pool <- struct{}{}
				}
			}()
		case <-b.GetStopChan():
			return nil
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *Broker) consumeOne(delivery *pubsub.Message, taskProcessor iface.TaskProcessor) error {
	if len(delivery.Data) == 0 {
		delivery.Nack()
		log.ERROR.Printf("received an empty message, the delivery was %v", delivery)
		return errors.New("Received an empty message")
	}

	sig := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewBuffer(delivery.Data))
	decoder.UseNumber()
	if err := decoder.Decode(sig); err != nil {
		delivery.Nack()
		log.ERROR.Printf("unmarshal error. the delivery is %v", delivery)
		return err
	}

	// If the task is not registered return an error
	// and leave the message in the queue
	if !b.IsTaskRegistered(sig.Name) {
		delivery.Nack()
		return fmt.Errorf("task %s is not registered", sig.Name)
	}

	err := taskProcessor.Process(sig)
	if err != nil {
		delivery.Nack()
		return err
	}

	// Call Ack() after successfully consuming and processing the message
	delivery.Ack()

	return err
}
