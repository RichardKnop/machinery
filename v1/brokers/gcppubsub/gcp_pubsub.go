package gcppubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

	stopDone chan struct{}
}

// New creates new Broker instance
func New(cnf *config.Config, projectID, subscriptionName string) (iface.Broker, error) {
	b := &Broker{Broker: common.NewBroker(cnf), stopDone: make(chan struct{})}
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

	sub := b.service.Subscription(b.subscriptionName)

	if b.MaxExtension != 0 {
		sub.ReceiveSettings.MaxExtension = b.MaxExtension
	}

	sub.ReceiveSettings.NumGoroutines = concurrency
	log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-b.GetStopChan()
		cancel()
	}()

	for {
		err := sub.Receive(ctx, func(_ctx context.Context, msg *pubsub.Message) {
			b.consumeOne(msg, taskProcessor)
		})
		if err == nil {
			break
		}

		log.ERROR.Printf("Error when receiving messages. Error: %v", err)
		continue
	}

	close(b.stopDone)

	return b.GetRetry(), nil
}

// StopConsuming quits the loop
func (b *Broker) StopConsuming() {
	b.Broker.StopConsuming()

	// Waiting for any tasks being processed to finish
	<-b.stopDone
}

// Publish places a new message on the default queue or the queue pointed to
// by the routing key
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) error {
	// Adjust routing key (this decides which queue the message will be published to)
	b.AdjustRoutingKey(signature)

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	topic := b.service.Topic(signature.RoutingKey)
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

// consumeOne processes a single message using TaskProcessor
func (b *Broker) consumeOne(delivery *pubsub.Message, taskProcessor iface.TaskProcessor) {
	if len(delivery.Data) == 0 {
		delivery.Nack()
		log.ERROR.Printf("received an empty message, the delivery was %v", delivery)
	}

	sig := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewBuffer(delivery.Data))
	decoder.UseNumber()
	if err := decoder.Decode(sig); err != nil {
		delivery.Nack()
		log.ERROR.Printf("unmarshal error. the delivery is %v", delivery)
	}

	// If the task is not registered return an error
	// and leave the message in the queue
	if !b.IsTaskRegistered(sig.Name) {
		delivery.Nack()
		log.ERROR.Printf("task %s is not registered", sig.Name)
	}

	err := taskProcessor.Process(sig)
	if err != nil {
		delivery.Nack()
		log.ERROR.Printf("Failed process of task", err)
	}

	// Call Ack() after successfully consuming and processing the message
	delivery.Ack()
}
