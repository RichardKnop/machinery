package integration_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/RichardKnop/machinery/v1/config"
)

func createGCPPubSubTopicAndSubscription(cli *pubsub.Client) {
	ctx := context.Background()

	var topic *pubsub.Topic

	topic = cli.Topic("test_queue")
	topicExists, err := topic.Exists(ctx)
	if err != nil {
		panic(err)
	}

	if !topicExists {
		topic, err = cli.CreateTopic(ctx, "test_queue")
		if err != nil {
			panic(err)
		}
	}

	var sub *pubsub.Subscription

	sub = cli.Subscription("test_queue")
	subExists, err := sub.Exists(ctx)
	if err != nil {
		panic(err)
	}

	if !subExists {
		sub, err = cli.CreateSubscription(ctx, "test_queue", pubsub.SubscriptionConfig{
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

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	// Redis broker, Redis result backend
	server := testSetup(&config.Config{
		Broker:        pubsubURL,
		DefaultQueue:  "test_queue",
		ResultBackend: fmt.Sprintf("redis://%v", redisURL),
	})

	// Create Cloud Pub/Sub Topic and Subscription
	createGCPPubSubTopicAndSubscription(server.GetBroker().GetConfig().GCPPubSub.Client)

	worker := server.NewWorker("test_worker", 0)
	go worker.Launch()
	testAll(server, t)
	worker.Quit()
}
