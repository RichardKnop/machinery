package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
)

func createGCPPubSubTopicAndSubscription(cli *pubsub.Client, topicName, subscriptionName string) {
	ctx := context.Background()

	var topic *pubsub.Topic

	topic = cli.Topic(topicName)
	topicExists, err := topic.Exists(ctx)
	if err != nil {
		panic(err)
	}

	if !topicExists {
		topic, err = cli.CreateTopic(ctx, topicName)
		if err != nil {
			panic(err)
		}
	}

	var sub *pubsub.Subscription

	sub = cli.Subscription(subscriptionName)
	subExists, err := sub.Exists(ctx)
	if err != nil {
		panic(err)
	}

	if !subExists {
		_, err = cli.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{
			Topic:       topic,
			AckDeadline: 10 * time.Second,
		})
		if err != nil {
			panic(err)
		}
	}
}

func TestGCPPubSubRedis(t *testing.T) {
	// start Cloud Pub/Sub emulator
	// $ LANG=C gcloud beta emulators pubsub start
	// $ eval $(LANG=C gcloud beta emulators pubsub env-init)

	pubsubURL := os.Getenv("GCPPUBSUB_URL")
	if pubsubURL == "" {
		t.Skip("GCPPUBSUB_URL is not defined")
	}

	topicName := os.Getenv("GCPPUBSUB_TOPIC")
	if topicName == "" {
		t.Skip("GCPPUBSUB_TOPIC is not defined")
	}

	_, subscriptionName, err := machinery.ParseGCPPubSubURL(pubsubURL)
	if err != nil {
		t.Fatal(err)
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	pubsubClient, err := pubsub.NewClient(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}

	// Create Cloud Pub/Sub Topic and Subscription
	createGCPPubSubTopicAndSubscription(pubsubClient, topicName, subscriptionName)

	// Redis broker, Redis result backend
	server := testSetup(&config.Config{
		Broker:        pubsubURL,
		DefaultQueue:  topicName,
		ResultBackend: fmt.Sprintf("redis://%v", redisURL),
		Lock:          fmt.Sprintf("redis://%v", redisURL),
		GCPPubSub: &config.GCPPubSubConfig{
			Client: pubsubClient,
		},
	})

	worker := server.(*machinery.Server).NewWorker("test_worker", 0)
	defer worker.Quit()
	go worker.Launch()
	testAll(server, t)
}
