package brokers

import (
	"encoding/json"
	"errors"
	"os"
	"sync"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

var (
	TestAWSSQSBroker     *AWSSQSBroker
	ErrAWSSQSBroker      *AWSSQSBroker
	ReceiveMessageOutput *sqs.ReceiveMessageOutput
	TestConf             *config.Config
)

type FakeSQS struct {
	sqsiface.SQSAPI
}

func (f *FakeSQS) SendMessage(*sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	output := sqs.SendMessageOutput{
		MD5OfMessageAttributes: aws.String("d25a6aea97eb8f585bfa92d314504a92"),
		MD5OfMessageBody:       aws.String("bbdc5fdb8be7251f5c910905db994bab"),
		MessageId:              aws.String("47f8b355-5115-4b45-b33a-439016400411"),
	}
	return &output, nil
}

func (f *FakeSQS) ReceiveMessage(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	return ReceiveMessageOutput, nil
}

func (f *FakeSQS) DeleteMessage(*sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	return &sqs.DeleteMessageOutput{}, nil
}

type ErrorSQS struct {
	sqsiface.SQSAPI
}

func (e *ErrorSQS) SendMessage(*sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	err := errors.New("this is an error")
	return nil, err
}

func (e *ErrorSQS) ReceiveMessage(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	err := errors.New("this is an error")
	return nil, err
}

func (e *ErrorSQS) DeleteMessage(*sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	err := errors.New("this is an error")
	return nil, err
}

func init() {
	redisURL := os.Getenv("REDIS_URL")
	brokerUrl := "https://sqs.foo.amazonaws.com.cn"
	TestConf = &config.Config{
		Broker:        brokerUrl,
		DefaultQueue:  "test_queue",
		ResultBackend: redisURL,
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	svc := new(FakeSQS)
	TestAWSSQSBroker = &AWSSQSBroker{
		Broker:            New(TestConf),
		sess:              sess,
		service:           svc,
		processingWG:      sync.WaitGroup{},
		receivingWG:       sync.WaitGroup{},
		stopReceivingChan: make(chan int),
	}

	errSvc := new(ErrorSQS)
	ErrAWSSQSBroker = &AWSSQSBroker{
		Broker:            New(TestConf),
		sess:              sess,
		service:           errSvc,
		processingWG:      sync.WaitGroup{},
		receivingWG:       sync.WaitGroup{},
		stopReceivingChan: make(chan int),
	}

	// TODO: chang message body to signature example
	messageBody, _ := json.Marshal(map[string]int{"apple": 5, "lettuce": 7})
	ReceiveMessageOutput = &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{
			{
				Attributes: map[string]*string{
					"SentTimestamp": aws.String("1512962021537"),
				},
				Body:                   aws.String(string(messageBody)),
				MD5OfBody:              aws.String("bbdc5fdb8be7251f5c910905db994bab"),
				MD5OfMessageAttributes: aws.String("d25a6aea97eb8f585bfa92d314504a92"),
				MessageAttributes: map[string]*sqs.MessageAttributeValue{
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

func (b *AWSSQSBroker) ConsumeForTesting(deliveries <-chan *sqs.ReceiveMessageOutput, concurrency int, taskProcessor TaskProcessor) error {
	return b.consume(deliveries, concurrency, taskProcessor)
}

func (b *AWSSQSBroker) ConsumeOneForTesting(delivery *sqs.ReceiveMessageOutput, taskProcessor TaskProcessor) error {
	return b.consumeOne(delivery, taskProcessor)
}

func (b *AWSSQSBroker) DeleteOneForTesting(delivery *sqs.ReceiveMessageOutput) error {
	return b.deleteOne(delivery)
}

func (b *AWSSQSBroker) DefaultQueueURLForTesting() *string {
	return b.defaultQueueURL()
}

func (b *AWSSQSBroker) ReceiveMessageForTesting(qURL *string) (*sqs.ReceiveMessageOutput, error) {
	return b.receiveMessage(qURL)
}

func (b *AWSSQSBroker) InitializePoolForTesting(pool chan struct{}, concurrency int) {
	b.initializePool(pool, concurrency)
}

func (b *AWSSQSBroker) ConsumeDeliveriesForTesting(deliveries <-chan *sqs.ReceiveMessageOutput, concurrency int, taskProcessor TaskProcessor, pool chan struct{}, errorsChan chan error) error {
	return b.consumeDeliveries(deliveries, concurrency, taskProcessor, pool, errorsChan)
}

func (b *AWSSQSBroker) ContinueReceivingMessagesForTesting(qURL *string, deliveries chan *sqs.ReceiveMessageOutput) (bool, error) {
	return b.continueReceivingMessages(qURL, deliveries)
}

func (b *AWSSQSBroker) StopReceivingForTesting() {
	b.stopReceiving()
}

func (b *AWSSQSBroker) GetStopReceivingChanForTesting() chan int {
	return b.stopReceivingChan
}

func (b *Broker) StartConsumingForTesting(consumerTag string, taskProcessor TaskProcessor) {
	b.startConsuming(consumerTag, taskProcessor)
}

func (b *Broker) GetRetryFuncForTesting() func(chan int) {
	return b.retryFunc
}

func (b *Broker) GetStopChanForTesting() chan int {
	return b.stopChan
}

func (b *Broker) GetRetryStopChanForTesting() chan int {
	return b.retryStopChan
}
