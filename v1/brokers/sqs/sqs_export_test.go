package sqs

import (
	"encoding/json"
	"errors"
	"os"
	"sync"

	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"

	awssqs "github.com/aws/aws-sdk-go/service/sqs"
)

var (
	TestAWSSQSBroker     *Broker
	ErrAWSSQSBroker      *Broker
	ReceiveMessageOutput *awssqs.ReceiveMessageOutput
	TestConf             *config.Config
)

type FakeSQS struct {
	sqsiface.SQSAPI
}

func (f *FakeSQS) SendMessage(*awssqs.SendMessageInput) (*awssqs.SendMessageOutput, error) {
	output := awssqs.SendMessageOutput{
		MD5OfMessageAttributes: aws.String("d25a6aea97eb8f585bfa92d314504a92"),
		MD5OfMessageBody:       aws.String("bbdc5fdb8be7251f5c910905db994bab"),
		MessageId:              aws.String("47f8b355-5115-4b45-b33a-439016400411"),
	}
	return &output, nil
}

func (f *FakeSQS) ReceiveMessage(*awssqs.ReceiveMessageInput) (*awssqs.ReceiveMessageOutput, error) {
	return ReceiveMessageOutput, nil
}

func (f *FakeSQS) DeleteMessage(*awssqs.DeleteMessageInput) (*awssqs.DeleteMessageOutput, error) {
	return &awssqs.DeleteMessageOutput{}, nil
}

type ErrorSQS struct {
	sqsiface.SQSAPI
}

func (e *ErrorSQS) SendMessage(*awssqs.SendMessageInput) (*awssqs.SendMessageOutput, error) {
	err := errors.New("this is an error")
	return nil, err
}

func (e *ErrorSQS) ReceiveMessage(*awssqs.ReceiveMessageInput) (*awssqs.ReceiveMessageOutput, error) {
	err := errors.New("this is an error")
	return nil, err
}

func (e *ErrorSQS) DeleteMessage(*awssqs.DeleteMessageInput) (*awssqs.DeleteMessageOutput, error) {
	err := errors.New("this is an error")
	return nil, err
}

func init() {
	redisURL := os.Getenv("REDIS_URL")
	brokerURL := "https://sqs.foo.amazonaws.com.cn"
	TestConf = &config.Config{
		Broker:        brokerURL,
		DefaultQueue:  "test_queue",
		ResultBackend: redisURL,
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	svc := new(FakeSQS)
	TestAWSSQSBroker = &Broker{
		Broker:            common.NewBroker(TestConf),
		sess:              sess,
		service:           svc,
		processingWG:      sync.WaitGroup{},
		receivingWG:       sync.WaitGroup{},
		stopReceivingChan: make(chan int),
	}

	errSvc := new(ErrorSQS)
	ErrAWSSQSBroker = &Broker{
		Broker:            common.NewBroker(TestConf),
		sess:              sess,
		service:           errSvc,
		processingWG:      sync.WaitGroup{},
		receivingWG:       sync.WaitGroup{},
		stopReceivingChan: make(chan int),
	}

	// TODO: chang message body to signature example
	messageBody, _ := json.Marshal(map[string]int{"apple": 5, "lettuce": 7})
	ReceiveMessageOutput = &awssqs.ReceiveMessageOutput{
		Messages: []*awssqs.Message{
			{
				Attributes: map[string]*string{
					"SentTimestamp": aws.String("1512962021537"),
				},
				Body:                   aws.String(string(messageBody)),
				MD5OfBody:              aws.String("bbdc5fdb8be7251f5c910905db994bab"),
				MD5OfMessageAttributes: aws.String("d25a6aea97eb8f585bfa92d314504a92"),
				MessageAttributes: map[string]*awssqs.MessageAttributeValue{
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
