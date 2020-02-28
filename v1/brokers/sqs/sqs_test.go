package sqs_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/brokers/sqs"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/retry"

	awssqs "github.com/aws/aws-sdk-go/service/sqs"
)

var (
	testBroker           iface.Broker
	testAWSSQSBroker     *sqs.Broker
	errAWSSQSBroker      *sqs.Broker
	cnf                  *config.Config
	receiveMessageOutput *awssqs.ReceiveMessageOutput
)

func init() {
	testAWSSQSBroker = sqs.TestAWSSQSBroker
	errAWSSQSBroker = sqs.ErrAWSSQSBroker
	cnf = sqs.TestConf
	receiveMessageOutput = sqs.ReceiveMessageOutput
	testBroker = sqs.New(cnf)
}

func TestNewAWSSQSBroker(t *testing.T) {
	assert.IsType(t, testAWSSQSBroker, testBroker)
}

func TestPrivateFunc_continueReceivingMessages(t *testing.T) {
	qURL := testAWSSQSBroker.DefaultQueueURLForTest()
	deliveries := make(chan *awssqs.ReceiveMessageOutput)
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
	receiveMessageOutput.Messages = []*awssqs.Message{}
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
	pool := make(chan struct{}, 0)
	wk := server1.NewWorker("sms_worker", 0)
	deliveries := make(chan *awssqs.ReceiveMessageOutput)
	outputCopy := *receiveMessageOutput
	outputCopy.Messages = []*awssqs.Message{}
	go func() { deliveries <- &outputCopy }()

	// an infinite loop will be executed only when there is no error
	err = testAWSSQSBroker.ConsumeForTest(deliveries, 0, wk, pool)
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
	outputCopy.Messages = []*awssqs.Message{}
	err = testAWSSQSBroker.ConsumeOneForTest(&outputCopy, wk)
	assert.NotNil(t, err)

	outputCopy.Messages = []*awssqs.Message{
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
	testAWSSQSBroker.StartConsumingForTest("fooTag", 1, wk)
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
	deliveries := make(chan *awssqs.ReceiveMessageOutput)
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
	outputCopy.Messages = []*awssqs.Message{}
	go func() { deliveries <- &outputCopy }()
	whetherContinue, err = testAWSSQSBroker.ConsumeDeliveriesForTest(deliveries, concurrency, wk, pool, errorsChan)
	e := <-errorsChan
	assert.True(t, whetherContinue)
	assert.NotNil(t, e)
	assert.Nil(t, err)

	// using a wait group and a channel to fix the racing problem
	var wg sync.WaitGroup
	wg.Add(1)
	nextStep := make(chan bool, 1)
	go func() {
		defer wg.Done()
		// nextStep <- true runs after defer wg.Done(), to make sure the next go routine runs after this go routine
		nextStep <- true
		deliveries <- receiveMessageOutput
	}()
	if <-nextStep {
		// <-pool will block the routine in the following steps, so pool <- struct{}{} will be executed for sure
		go func() { wg.Wait(); pool <- struct{}{} }()
	}
	whetherContinue, err = testAWSSQSBroker.ConsumeDeliveriesForTest(deliveries, concurrency, wk, pool, errorsChan)
	// the pool shouldn't be consumed
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

func Test_CustomQueueName(t *testing.T) {
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}

	wk := server1.NewWorker("test-worker", 0)
	qURL := testAWSSQSBroker.GetQueueURLForTest(wk)
	assert.Equal(t, qURL, testAWSSQSBroker.DefaultQueueURLForTest(), "")

	wk2 := server1.NewCustomQueueWorker("test-worker", 0, "my-custom-queue")
	qURL2 := testAWSSQSBroker.GetQueueURLForTest(wk2)
	assert.Equal(t, qURL2, testAWSSQSBroker.GetCustomQueueURL("my-custom-queue"), "")
}

func TestPrivateFunc_consumeWithConcurrency(t *testing.T) {

	msg := `{
        "UUID": "uuid-dummy-task",
        "Name": "test-task",
        "RoutingKey": "dummy-routing"
	}
	`

	testResp := "47f8b355-5115-4b45-b33a-439016400411"
	output := make(chan string) // The output channel

	cnf.ResultBackend = "eager"
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}
	err = server1.RegisterTask("test-task", func(ctx context.Context) error {
		output <- testResp

		return nil
	})
	testAWSSQSBroker.SetRegisteredTaskNames([]string{"test-task"})
	assert.NoError(t, err)
	pool := make(chan struct{}, 1)
	pool <- struct{}{}
	wk := server1.NewWorker("sms_worker", 1)
	deliveries := make(chan *awssqs.ReceiveMessageOutput)
	outputCopy := *receiveMessageOutput
	outputCopy.Messages = []*awssqs.Message{
		{
			MessageId: aws.String("test-sqs-msg1"),
			Body:      aws.String(msg),
		},
	}

	go func() {
		deliveries <- &outputCopy

	}()

	go func() {
		err = testAWSSQSBroker.ConsumeForTest(deliveries, 1, wk, pool)
	}()

	select {
	case resp := <-output:
		assert.Equal(t, testResp, resp)

	case <-time.After(10 * time.Second):
		// call timed out
		t.Fatal("task not processed in 10 seconds")
	}
}
