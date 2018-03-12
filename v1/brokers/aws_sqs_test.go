package brokers_test

import (
	"errors"
	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/brokers"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/retry"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var (
	testBroker           brokers.Interface
	testAWSSQSBroker     *brokers.AWSSQSBroker
	errAWSSQSBroker      *brokers.AWSSQSBroker
	cnf                  *config.Config
	receiveMessageOutput *sqs.ReceiveMessageOutput
)

func init() {
	testAWSSQSBroker = brokers.TestAWSSQSBroker
	errAWSSQSBroker = brokers.ErrAWSSQSBroker
	cnf = brokers.TestConf
	receiveMessageOutput = brokers.ReceiveMessageOutput
	testBroker = brokers.NewAWSSQSBroker(cnf)
}

func TestNewAWSSQSBroker(t *testing.T) {
	assert.IsType(t, testAWSSQSBroker, testBroker)
}

func TestPrivateFunc_continueReceivingMessages(t *testing.T) {
	qURL := testAWSSQSBroker.DefaultQueueURLForTest()
	deliveries := make(chan *sqs.ReceiveMessageOutput)
	firstStep := make(chan int)
	nextStep := make(chan int)
	go func() {
		stopReceivingChan := testAWSSQSBroker.GetStopReceivingChanForTest()
		firstStep <- 1
		stopReceivingChan <- 1
	}()

	var (
		whetherContinue bool
		err             error
	)
	<-firstStep
	// Test the case that a signal was received from stopReceivingChan
	go func() {
		whetherContinue, err = testAWSSQSBroker.ContinueReceivingMessagesForTest(qURL, deliveries)
		nextStep <- 1
	}()
	<-nextStep
	assert.False(t, whetherContinue)
	assert.Nil(t, err)

	// Test the default condition
	whetherContinue, err = testAWSSQSBroker.ContinueReceivingMessagesForTest(qURL, deliveries)
	assert.True(t, whetherContinue)
	assert.Nil(t, err)

	// Test the error
	whetherContinue, err = errAWSSQSBroker.ContinueReceivingMessagesForTest(qURL, deliveries)
	assert.True(t, whetherContinue)
	assert.NotNil(t, err)

	// Test when there is no message
	outputCopy := *receiveMessageOutput
	receiveMessageOutput.Messages = []*sqs.Message{}
	whetherContinue, err = testAWSSQSBroker.ContinueReceivingMessagesForTest(qURL, deliveries)
	assert.True(t, whetherContinue)
	assert.Nil(t, err)
	// recover original value
	*receiveMessageOutput = outputCopy

}

func TestPrivateFunc_consume(t *testing.T) {
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}
	wk := server1.NewWorker("sms_worker", 0)
	deliveries := make(chan *sqs.ReceiveMessageOutput)
	outputCopy := *receiveMessageOutput
	outputCopy.Messages = []*sqs.Message{}
	go func() { deliveries <- &outputCopy }()

	// an infinite loop will be executed only when there is no error
	err = testAWSSQSBroker.ConsumeForTest(deliveries, 0, wk)
	assert.NotNil(t, err)

}

func TestPrivateFunc_consumeOne(t *testing.T) {
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}
	wk := server1.NewWorker("sms_worker", 0)
	err = testAWSSQSBroker.ConsumeOneForTest(receiveMessageOutput, wk)
	assert.NotNil(t, err)

	outputCopy := *receiveMessageOutput
	outputCopy.Messages = []*sqs.Message{}
	err = testAWSSQSBroker.ConsumeOneForTest(&outputCopy, wk)
	assert.NotNil(t, err)

	outputCopy.Messages = []*sqs.Message{
		{
			Body: aws.String("foo message"),
		},
	}
	err = testAWSSQSBroker.ConsumeOneForTest(&outputCopy, wk)
	assert.NotNil(t, err)
}

func TestPrivateFunc_initializePool(t *testing.T) {
	concurrency := 9
	pool := make(chan struct{}, concurrency)
	testAWSSQSBroker.InitializePoolForTest(pool, concurrency)
	assert.Len(t, pool, concurrency)
}

func TestPrivateFunc_startConsuming(t *testing.T) {
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}

	wk := server1.NewWorker("sms_worker", 0)
	retryFunc := testAWSSQSBroker.GetRetryFuncForTest()
	stopChan := testAWSSQSBroker.GetStopChanForTest()
	retryStopChan := testAWSSQSBroker.GetRetryStopChanForTest()
	assert.Nil(t, retryFunc)
	testAWSSQSBroker.StartConsumingForTest("fooTag", wk)
	assert.IsType(t, retryFunc, retry.Closure())
	assert.Equal(t, len(stopChan), 0)
	assert.Equal(t, len(retryStopChan), 0)
}

func TestPrivateFuncDefaultQueueURL(t *testing.T) {
	qURL := testAWSSQSBroker.DefaultQueueURLForTest()

	assert.EqualValues(t, *qURL, "https://sqs.foo.amazonaws.com.cn/test_queue")
}

func TestPrivateFunc_stopReceiving(t *testing.T) {
	go testAWSSQSBroker.StopReceivingForTest()
	stopReceivingChan := testAWSSQSBroker.GetStopReceivingChanForTest()
	assert.NotNil(t, <-stopReceivingChan)
}

func TestPrivateFunc_receiveMessage(t *testing.T) {
	qURL := testAWSSQSBroker.DefaultQueueURLForTest()
	output, err := testAWSSQSBroker.ReceiveMessageForTest(qURL)
	assert.Nil(t, err)
	assert.Equal(t, receiveMessageOutput, output)
}

func TestPrivateFunc_consumeDeliveries(t *testing.T) {
	concurrency := 0
	pool := make(chan struct{}, concurrency)
	errorsChan := make(chan error)
	deliveries := make(chan *sqs.ReceiveMessageOutput, 0)
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}
	wk := server1.NewWorker("sms_worker", 0)
	go func() { deliveries <- receiveMessageOutput }()
	whetherContinue, err := testAWSSQSBroker.ConsumeDeliveriesForTest(deliveries, concurrency, wk, pool, errorsChan)
	assert.True(t, whetherContinue)
	assert.Nil(t, err)

	go func() { errorsChan <- errors.New("foo error") }()
	whetherContinue, err = testAWSSQSBroker.ConsumeDeliveriesForTest(deliveries, concurrency, wk, pool, errorsChan)
	assert.False(t, whetherContinue)
	assert.NotNil(t, err)

	go func() { testAWSSQSBroker.GetStopChanForTest() <- 1 }()
	whetherContinue, err = testAWSSQSBroker.ConsumeDeliveriesForTest(deliveries, concurrency, wk, pool, errorsChan)
	assert.False(t, whetherContinue)
	assert.Nil(t, err)

	outputCopy := *receiveMessageOutput
	outputCopy.Messages = []*sqs.Message{}
	go func() { deliveries <- &outputCopy }()
	whetherContinue, err = testAWSSQSBroker.ConsumeDeliveriesForTest(deliveries, concurrency, wk, pool, errorsChan)
	e := <-errorsChan
	assert.True(t, whetherContinue)
	assert.NotNil(t, e)
	assert.Nil(t, err)

	go func() { deliveries <- receiveMessageOutput }()
	// wait for a while to fix racing problem
	time.Sleep(5 * time.Second)
	go func() { pool <- struct{}{} }()
	whetherContinue, err = testAWSSQSBroker.ConsumeDeliveriesForTest(deliveries, concurrency, wk, pool, errorsChan)
	p := <-pool
	assert.True(t, whetherContinue)
	assert.NotNil(t, p)
	assert.Nil(t, err)
}

func TestPrivateFunc_deleteOne(t *testing.T) {
	err := testAWSSQSBroker.DeleteOneForTest(receiveMessageOutput)
	assert.Nil(t, err)

	err = errAWSSQSBroker.DeleteOneForTest(receiveMessageOutput)
	assert.NotNil(t, err)
}
