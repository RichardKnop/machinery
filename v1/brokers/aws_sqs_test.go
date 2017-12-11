package brokers

import (
	"testing"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/retry"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	//"fmt"
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/aws"
)

var (
	testBroker           Interface
	testAWSSQSBroker     *AWSSQSBroker
	errAWSSQSBroker      *AWSSQSBroker
	cnf                  *config.Config
	receiveMessageOutput *sqs.ReceiveMessageOutput
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
	return receiveMessageOutput, nil
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
	cnf = &config.Config{
		Broker:        brokerUrl,
		DefaultQueue:  "test_queue",
		ResultBackend: redisURL,
	}
	testBroker = NewAWSSQSBroker(cnf)

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	svc := new(FakeSQS)
	testAWSSQSBroker = &AWSSQSBroker{
		Broker:            New(cnf),
		sess:              sess,
		service:           svc,
		processingWG:      sync.WaitGroup{},
		receivingWG:       sync.WaitGroup{},
		stopReceivingChan: make(chan int),
	}

	errSvc := new(ErrorSQS)
	errAWSSQSBroker = &AWSSQSBroker{
		Broker:            New(cnf),
		sess:              sess,
		service:           errSvc,
		processingWG:      sync.WaitGroup{},
		receivingWG:       sync.WaitGroup{},
		stopReceivingChan: make(chan int),
	}

	messageBody, _ := json.Marshal(map[string]int{"apple": 5, "lettuce": 7})
	receiveMessageOutput = &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{
			&sqs.Message{
				Attributes: map[string]*string{
					"SentTimestamp": aws.String("1512962021537"),
				},
				Body:                   aws.String(string(messageBody)),
				MD5OfBody:              aws.String("bbdc5fdb8be7251f5c910905db994bab"),
				MD5OfMessageAttributes: aws.String("d25a6aea97eb8f585bfa92d314504a92"),
				MessageAttributes: map[string]*sqs.MessageAttributeValue{
					"Title": &sqs.MessageAttributeValue{
						DataType:    aws.String("String"),
						StringValue: aws.String("The Whistler"),
					},
					"Author": &sqs.MessageAttributeValue{
						DataType:    aws.String("String"),
						StringValue: aws.String("John Grisham"),
					},
					"WeeksOn": &sqs.MessageAttributeValue{
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

func TestNewAWSSQSBroker(t *testing.T) {
	assert.IsType(t, testAWSSQSBroker, testBroker)
}

func TestStartConsuming(t *testing.T) {
	//region := os.Getenv("AWS_REGION")
	//if region == "" {
	//	return
	//}
	//server1, err := machinery.NewServer(cnf)
	//if err != nil {
	//	t.Fatal(err)
	//}

	//wk := server1.NewWorker("sms_worker", 0)
	// stop receiving
	//go func() { testAWSSQSBroker.stopReceivingChan <- 1 }()
	//testAWSSQSBroker.StartConsuming("fooTag", 0, wk)
}

func TestPrivateFunc_consume(t *testing.T) {
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}
	wk := server1.NewWorker("sms_worker", 0)
	deliveries := make(chan *sqs.ReceiveMessageOutput)
	outputCopy := receiveMessageOutput
	outputCopy.Messages = []*sqs.Message{}
	go func() { deliveries <- outputCopy }()

	// an infinite loop will be executed only when there is no error
	err = testAWSSQSBroker.consume(deliveries, 0, wk)
	assert.NotNil(t, err)

}

func TestPrivateFunc_consumeOne(t *testing.T) {
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}
	wk := server1.NewWorker("sms_worker", 0)
	err = testAWSSQSBroker.consumeOne(receiveMessageOutput, wk)
	assert.Nil(t, err)

	outputCopy := receiveMessageOutput
	outputCopy.Messages = []*sqs.Message{}
	err = testAWSSQSBroker.consumeOne(receiveMessageOutput, wk)
	assert.NotNil(t, err)

	outputCopy.Messages = []*sqs.Message{
		&sqs.Message{
			Body: aws.String("foo message"),
		},
	}
	err = testAWSSQSBroker.consumeOne(receiveMessageOutput, wk)
	assert.NotNil(t, err)

	// TODO: test requeue and deleteOne
}

func TestPrivateFunc_initializePool(t *testing.T) {
	concurrency := 9
	pool := make(chan struct{}, concurrency)
	testAWSSQSBroker.initializePool(pool, concurrency)
	assert.Len(t, pool, concurrency)
}

func TestPrivateFunc_startConsuming(t *testing.T) {
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}

	wk := server1.NewWorker("sms_worker", 0)
	assert.Nil(t, testAWSSQSBroker.retryFunc)
	testAWSSQSBroker.startConsuming("fooTag", wk)
	assert.IsType(t, testAWSSQSBroker.retryFunc, retry.Closure())
	assert.Equal(t, len(testAWSSQSBroker.stopChan), 0)
	assert.Equal(t, len(testAWSSQSBroker.retryStopChan), 0)
}

func TestPrivateFuncDefaultQueueURL(t *testing.T) {
	qURL := testAWSSQSBroker.defaultQueueURL()

	assert.EqualValues(t, *qURL, "https://sqs.foo.amazonaws.com.cn/test_queue")
}

func TestStopConsuming(t *testing.T) {

}

func TestPrivateFunc_receiveMessage(t *testing.T) {
	qURL := testAWSSQSBroker.defaultQueueURL()
	output, err := testAWSSQSBroker.receiveMessage(qURL)
	assert.Nil(t, err)
	assert.Equal(t, receiveMessageOutput, output)
}

func TestPrivateFunc_consumeDeliveries(t *testing.T) {
	concurrency := 0
	pool := make(chan struct{}, concurrency)
	errorsChan := make(chan error)
	deliveries := make(chan *sqs.ReceiveMessageOutput)
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}
	wk := server1.NewWorker("sms_worker", 0)
	go func() { deliveries <- receiveMessageOutput }()
	err = testAWSSQSBroker.consumeDeliveries(deliveries, concurrency, wk, pool, errorsChan)

	assert.Nil(t, err)

	go func() { errorsChan <- errors.New("foo error") }()
	err = testAWSSQSBroker.consumeDeliveries(deliveries, concurrency, wk, pool, errorsChan)
	assert.NotNil(t, err)

	go func() { testAWSSQSBroker.stopChan <- 1 }()
	err = testAWSSQSBroker.consumeDeliveries(deliveries, concurrency, wk, pool, errorsChan)
	assert.Nil(t, err)

	outputCopy := receiveMessageOutput
	outputCopy.Messages = []*sqs.Message{}
	go func() { deliveries <- outputCopy }()
	err = testAWSSQSBroker.consumeDeliveries(deliveries, concurrency, wk, pool, errorsChan)
	e := <-errorsChan
	assert.NotNil(t, e)
	assert.Nil(t, err)

	go func() { pool <- struct{}{} }()
	go func() { deliveries <- receiveMessageOutput }()
	err = testAWSSQSBroker.consumeDeliveries(deliveries, concurrency, wk, pool, errorsChan)
	p := <-pool
	assert.NotNil(t, p)
	assert.Nil(t, err)
}
