package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	sqsiface "github.com/RichardKnop/machinery/v2/brokers/iface/sqs"

	"github.com/RichardKnop/machinery/v2/brokers/iface"
	"github.com/RichardKnop/machinery/v2/common"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

var (
	ReceiveMessageOutput *awssqs.ReceiveMessageOutput
)

type FakeSQS struct {
	sqsiface.API
}

func (f *FakeSQS) SendMessage(context.Context, *awssqs.SendMessageInput, ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	output := awssqs.SendMessageOutput{
		MD5OfMessageAttributes: aws.String("d25a6aea97eb8f585bfa92d314504a92"),
		MD5OfMessageBody:       aws.String("bbdc5fdb8be7251f5c910905db994bab"),
		MessageId:              aws.String("47f8b355-5115-4b45-b33a-439016400411"),
	}
	return &output, nil
}

func (f *FakeSQS) ReceiveMessage(context.Context, *awssqs.ReceiveMessageInput, ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	return ReceiveMessageOutput, nil
}

func (f *FakeSQS) DeleteMessage(context.Context, *awssqs.DeleteMessageInput, ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	return &awssqs.DeleteMessageOutput{}, nil
}

type ErrorSQS struct {
	sqsiface.API
}

func (e *ErrorSQS) SendMessage(context.Context, *awssqs.SendMessageInput, ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	err := errors.New("this is an error")
	return nil, err
}

func (e *ErrorSQS) ReceiveMessage(context.Context, *awssqs.ReceiveMessageInput, ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	err := errors.New("this is an error")
	return nil, err
}

func (e *ErrorSQS) DeleteMessage(context.Context, *awssqs.DeleteMessageInput, ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	err := errors.New("this is an error")
	return nil, err
}

func init() {
	// TODO: chang message body to signature example
	messageBody, _ := json.Marshal(map[string]int{"apple": 5, "lettuce": 7})
	ReceiveMessageOutput = &awssqs.ReceiveMessageOutput{
		Messages: []types.Message{
			{
				Attributes: map[string]string{
					"SentTimestamp": "1512962021537",
				},
				Body:                   aws.String(string(messageBody)),
				MD5OfBody:              aws.String("bbdc5fdb8be7251f5c910905db994bab"),
				MD5OfMessageAttributes: aws.String("d25a6aea97eb8f585bfa92d314504a92"),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"Title": {
						DataType:    aws.String("String"),
						StringValue: aws.String("The Whistler"),
					},
					"Author": {
						DataType:    aws.String("String"),
						StringValue: aws.String("John Grisham"),
					},
					"WeeksOn": {
						DataType:    aws.String("Number"),
						StringValue: aws.String("6"),
					},
				},
				MessageId:     aws.String("47f8b355-5115-4b45-b33a-439016400411"),
				ReceiptHandle: aws.String("AQEBGhTR/nhq+pDPAunCDgLpwQuCq0JkD2dtv7pAcPF5DA/XaoPAjHfgn/PZ5DeG3YiQdTjCUj+rvFq5b79DTq+hK6r1Niuds02l+jdIk3u2JiL01Dsd203pW1lLUNryd74QAcn462eXzv7/hVDagXTn+KtOzox3X0vmPkCSQkWXWxtc23oa5+5Q7HWDmRm743L0zza1579rQ2R2B0TrdlTMpNsdjQlDmybNu+aDq8bazD/Wew539tIvUyYADuhVyKyS1L2QQuyXll73/DixulPNmvGPRHNoB1GIo+Ex929OHFchXoKonoFJnurX4VNNl1p/Byp2IYBi6nkTRzeJUFCrFq0WMAHKLwuxciezJSlLD7g3bbU8kgEer8+jTz1DBriUlDGsARr0s7mnlsd02cb46K/j+u1oPfA69vIVc0FaRtA="),
			},
		},
	}
}

func NewTestConfig() *config.Config {

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "eager"
	}
	brokerURL := "https://sqs.foo.amazonaws.com.cn"
	return &config.Config{
		Broker:        brokerURL,
		DefaultQueue:  "test_queue",
		ResultBackend: fmt.Sprintf("redis://%v", redisURL),
		Lock:          fmt.Sprintf("redis://%v", redisURL),
		SQS: &config.SQSConfig{
			VisibilityTimeout: aws.Int(30),
		},
	}
}

func NewTestBroker(cnf *config.Config) *Broker {

	var svc sqsiface.API = new(FakeSQS)

	if cnf.SQS.Client != nil {
		svc = cnf.SQS.Client
	}
	return &Broker{
		Broker:            common.NewBroker(cnf),
		service:           svc,
		processingWG:      sync.WaitGroup{},
		receivingWG:       sync.WaitGroup{},
		stopReceivingChan: make(chan int),
	}
}

func NewTestErrorBroker() *Broker {

	cnf := NewTestConfig()

	errSvc := new(ErrorSQS)
	return &Broker{
		Broker:            common.NewBroker(cnf),
		service:           errSvc,
		processingWG:      sync.WaitGroup{},
		receivingWG:       sync.WaitGroup{},
		stopReceivingChan: make(chan int),
	}
}

func (b *Broker) ConsumeForTest(deliveries <-chan *awssqs.ReceiveMessageOutput, concurrency int, taskProcessor iface.TaskProcessor, pool chan struct{}) error {
	return b.consume(deliveries, concurrency, taskProcessor, pool)
}

func (b *Broker) ConsumeOneForTest(delivery *awssqs.ReceiveMessageOutput, taskProcessor iface.TaskProcessor) error {
	return b.consumeOne(delivery, taskProcessor)
}

func (b *Broker) DeleteOneForTest(delivery *awssqs.ReceiveMessageOutput) error {
	return b.deleteOne(delivery)
}

func (b *Broker) DefaultQueueURLForTest() *string {
	return b.defaultQueueURL()
}

func (b *Broker) ReceiveMessageForTest(qURL *string) (*awssqs.ReceiveMessageOutput, error) {
	return b.receiveMessage(qURL)
}

func (b *Broker) InitializePoolForTest(pool chan struct{}, concurrency int) {
	b.initializePool(pool, concurrency)
}

func (b *Broker) ConsumeDeliveriesForTest(deliveries <-chan *awssqs.ReceiveMessageOutput, concurrency int, taskProcessor iface.TaskProcessor, pool chan struct{}, errorsChan chan error) (bool, error) {
	return b.consumeDeliveries(deliveries, concurrency, taskProcessor, pool, errorsChan)
}

func (b *Broker) ContinueReceivingMessagesForTest(qURL *string, deliveries chan *awssqs.ReceiveMessageOutput) (bool, error) {
	return b.continueReceivingMessages(qURL, deliveries)
}

func (b *Broker) StopReceivingForTest() {
	b.stopReceiving()
}

func (b *Broker) GetStopReceivingChanForTest() chan int {
	return b.stopReceivingChan
}

func (b *Broker) StartConsumingForTest(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) {
	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)
}

func (b *Broker) GetRetryFuncForTest() func(chan int) {
	return b.GetRetryFunc()
}

func (b *Broker) GetStopChanForTest() chan int {
	return b.GetStopChan()
}

func (b *Broker) GetRetryStopChanForTest() chan int {
	return b.GetRetryStopChan()
}

func (b *Broker) GetQueueURLForTest(taskProcessor iface.TaskProcessor) *string {
	return b.getQueueURL(taskProcessor)
}

func (b *Broker) GetCustomQueueURL(customQueue string) *string {
	return aws.String(b.GetConfig().Broker + "/" + customQueue)
}
