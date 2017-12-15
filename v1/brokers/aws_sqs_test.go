package brokers_test

import (
	"testing"

	"errors"
	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/brokers"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/retry"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/assert"
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
	qURL := testAWSSQSBroker.DefaultQueueURLForTesting()
	deliveries := make(chan *sqs.ReceiveMessageOutput)
	nextStep := make(chan int)
	go func() {
		stopReceivingChan := testAWSSQSBroker.GetStopReceivingChanForTesting()
		stopReceivingChan <- 1
	}()

	var (
		whetherContinue bool
		err             error
	)
	go func() {
		whetherContinue, err = testAWSSQSBroker.ContinueReceivingMessagesForTesting(qURL, deliveries)
		nextStep <- 1
	}()
	assert.False(t, whetherContinue)
	assert.Nil(t, err)

	<-nextStep
	whetherContinue, err = testAWSSQSBroker.ContinueReceivingMessagesForTesting(qURL, deliveries)
	assert.True(t, whetherContinue)
	assert.Nil(t, err)

	whetherContinue, err = errAWSSQSBroker.ContinueReceivingMessagesForTesting(qURL, deliveries)
	assert.True(t, whetherContinue)
	assert.NotNil(t, err)

	outputCopy := *receiveMessageOutput
	receiveMessageOutput.Messages = []*sqs.Message{}
	whetherContinue, err = testAWSSQSBroker.ContinueReceivingMessagesForTesting(qURL, deliveries)
	assert.True(t, whetherContinue)
	assert.Nil(t, err)
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
	err = testAWSSQSBroker.ConsumeForTesting(deliveries, 0, wk)
	assert.NotNil(t, err)

}

func TestPrivateFunc_consumeOne(t *testing.T) {
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}
	wk := server1.NewWorker("sms_worker", 0)
	err = testAWSSQSBroker.ConsumeOneForTesting(receiveMessageOutput, wk)
	assert.Nil(t, err)

	outputCopy := *receiveMessageOutput
	outputCopy.Messages = []*sqs.Message{}
	err = testAWSSQSBroker.ConsumeOneForTesting(&outputCopy, wk)
	assert.NotNil(t, err)

	outputCopy.Messages = []*sqs.Message{
		&sqs.Message{
			Body: aws.String("foo message"),
		},
	}
	err = testAWSSQSBroker.ConsumeOneForTesting(&outputCopy, wk)
	assert.NotNil(t, err)
}

func TestPrivateFunc_initializePool(t *testing.T) {
	concurrency := 9
	pool := make(chan struct{}, concurrency)
	testAWSSQSBroker.InitializePoolForTesting(pool, concurrency)
	assert.Len(t, pool, concurrency)
}

func TestPrivateFunc_startConsuming(t *testing.T) {
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}

	wk := server1.NewWorker("sms_worker", 0)
	retryFunc := testAWSSQSBroker.GetRetryFuncForTesting()
	stopChan := testAWSSQSBroker.GetStopChanForTesting()
	retryStopChan := testAWSSQSBroker.GetRetryStopChanForTesting()
	assert.Nil(t, retryFunc)
	testAWSSQSBroker.StartConsumingForTesting("fooTag", wk)
	assert.IsType(t, retryFunc, retry.Closure())
	assert.Equal(t, len(stopChan), 0)
	assert.Equal(t, len(retryStopChan), 0)
}

func TestPrivateFuncDefaultQueueURL(t *testing.T) {
	qURL := testAWSSQSBroker.DefaultQueueURLForTesting()

	assert.EqualValues(t, *qURL, "https://sqs.foo.amazonaws.com.cn/test_queue")
}

func TestPrivateFunc_stopReceiving(t *testing.T) {
	go testAWSSQSBroker.StopReceivingForTesting()
	stopReceivingChan := testAWSSQSBroker.GetStopReceivingChanForTesting()
	assert.NotNil(t, <-stopReceivingChan)
}

func TestPrivateFunc_receiveMessage(t *testing.T) {
	qURL := testAWSSQSBroker.DefaultQueueURLForTesting()
	output, err := testAWSSQSBroker.ReceiveMessageForTesting(qURL)
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
	err = testAWSSQSBroker.ConsumeDeliveriesForTesting(deliveries, concurrency, wk, pool, errorsChan)

	assert.Nil(t, err)

	go func() { errorsChan <- errors.New("foo error") }()
	err = testAWSSQSBroker.ConsumeDeliveriesForTesting(deliveries, concurrency, wk, pool, errorsChan)
	assert.NotNil(t, err)

	go func() { testAWSSQSBroker.GetStopChanForTesting() <- 1 }()
	err = testAWSSQSBroker.ConsumeDeliveriesForTesting(deliveries, concurrency, wk, pool, errorsChan)
	assert.Nil(t, err)

	outputCopy := *receiveMessageOutput
	outputCopy.Messages = []*sqs.Message{}
	go func() { deliveries <- &outputCopy }()
	err = testAWSSQSBroker.ConsumeDeliveriesForTesting(deliveries, concurrency, wk, pool, errorsChan)
	e := <-errorsChan
	assert.NotNil(t, e)
	assert.Nil(t, err)

	go func() { pool <- struct{}{} }()
	go func() { deliveries <- receiveMessageOutput }()
	err = testAWSSQSBroker.ConsumeDeliveriesForTesting(deliveries, concurrency, wk, pool, errorsChan)
	p := <-pool
	assert.NotNil(t, p)
	assert.Nil(t, err)
}

func TestPrivateFunc_deleteOne(t *testing.T) {
	err := testAWSSQSBroker.DeleteOneForTesting(receiveMessageOutput)
	assert.Nil(t, err)

	err = errAWSSQSBroker.DeleteOneForTesting(receiveMessageOutput)
	assert.NotNil(t, err)
}
