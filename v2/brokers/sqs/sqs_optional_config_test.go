package sqs_test

import (
	"strings"
	"testing"

	"github.com/RichardKnop/machinery/v2/brokers/sqs"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func TestConsumeOneWithoutSQSConfig(t *testing.T) {
	broker, err := sqs.New(&config.Config{
		Broker: "http://127.0.0.1/123456789012", DefaultQueue: "default-queue",
	})
	if err != nil {
		t.Fatal(err)
	}
	// An unregistered task must be rejected before any processor or SQS call.
	err = broker.(*sqs.Broker).ConsumeOneForTest(&awssqs.ReceiveMessageOutput{
		Messages: []types.Message{{Body: aws.String(`{"Name":"unregistered"}`)}},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("consume unregistered task = %v; want registration error", err)
	}
}
