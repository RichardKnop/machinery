package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/RichardKnop/machinery/v2/brokers/errs"
	"github.com/RichardKnop/machinery/v2/brokers/iface"
	sqsiface "github.com/RichardKnop/machinery/v2/brokers/iface/sqs"
	"github.com/RichardKnop/machinery/v2/common"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/log"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/session"
)

const (
	maxAWSSQSDelay = time.Minute * 15 // Max supported SQS delay is 15 min: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
)

// Broker represents a AWS SQS broker
// There are examples on: https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/sqs-example-create-queue.html
type Broker struct {
	common.Broker
	processingWG      sync.WaitGroup // use wait group to make sure task processing completes on interrupt signal
	receivingWG       sync.WaitGroup
	stopReceivingChan chan int
	sess              *session.Session
	service           sqsiface.API
	queueUrl          *string
}

// New creates new Broker instance
func New(cnf *config.Config) (iface.Broker, error) {
	b := &Broker{Broker: common.NewBroker(cnf)}
	if cnf.SQS != nil && cnf.SQS.Client != nil {
		// Use provided *SQS client
		b.service = cnf.SQS.Client
	} else {
		cfg, err := awsconfig.LoadDefaultConfig(context.TODO())
		if err != nil {
			return nil, fmt.Errorf("%w: unable to load AWS SDK config: ", err)
		}

		b.service = sqs.NewFromConfig(cfg)
	}

	return b, nil
}

// StartConsuming enters a loop and waits for incoming messages
func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)
	qURL := b.getQueueURL(taskProcessor)
	//save it so that it can be used later when attempting to delete task
	b.queueUrl = qURL

	deliveries := make(chan *sqs.ReceiveMessageOutput, concurrency)
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}
	b.stopReceivingChan = make(chan int)
	b.receivingWG.Add(1)

	go func() {
		defer b.receivingWG.Done()

		log.INFO.Printf("[*] Waiting for messages on queue: %s. To exit press CTRL+C\n", *qURL)

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.stopReceivingChan:
				close(deliveries)
				return
			case <-pool:
				output, err := b.receiveMessage(qURL)
				if err == nil && len(output.Messages) > 0 {
					deliveries <- output

				} else {
					//return back to pool right away
					pool <- struct{}{}
					if err != nil {
						log.ERROR.Printf("Queue consume error: %s", err)
					}

				}
			}

		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor, pool); err != nil {
		return b.GetRetry(), err
	}

	return b.GetRetry(), nil
}

// StopConsuming quits the loop
func (b *Broker) StopConsuming() {
	b.Broker.StopConsuming()

	b.stopReceiving()

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	// Waiting for the receiving goroutine to have stopped
	b.receivingWG.Wait()
}

// Publish places a new message on the default queue
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) error {
	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// Check that signature.RoutingKey is set, if not switch to DefaultQueue
	b.AdjustRoutingKey(signature)

	MsgInput := &sqs.SendMessageInput{
		MessageBody: aws.String(string(msg)),
		QueueUrl:    aws.String(b.GetConfig().Broker + "/" + signature.RoutingKey),
	}

	// if this is a fifo queue, there needs to be some additional parameters.
	if strings.HasSuffix(signature.RoutingKey, ".fifo") {
		// Use Machinery's signature Task UUID as SQS Message Group ID.
		MsgDedupID := signature.UUID
		MsgInput.MessageDeduplicationId = aws.String(MsgDedupID)

		// Do not Use Machinery's signature Group UUID as SQS Message Group ID, instead use BrokerMessageGroupId
		MsgGroupID := signature.BrokerMessageGroupId
		if MsgGroupID == "" {
			return fmt.Errorf("please specify BrokerMessageGroupId attribute for task Signature when submitting a task to FIFO queue")
		}
		MsgInput.MessageGroupId = aws.String(MsgGroupID)
	}

	// Check the ETA signature field, if it is set and it is in the future,
	// and is not a fifo queue, set a delay in seconds for the task.
	if signature.ETA != nil && !strings.HasSuffix(signature.RoutingKey, ".fifo") {
		now := time.Now().UTC()
		delay := signature.ETA.Sub(now)
		if delay > 0 {
			if delay > maxAWSSQSDelay {
				return errors.New("Max AWS SQS delay exceeded")
			}
			MsgInput.DelaySeconds = int32(delay.Seconds())
		}
	}

	result, err := b.service.SendMessage(ctx, MsgInput)

	if err != nil {
		log.ERROR.Printf("Error when sending a message: %v", err)
		return err

	}
	log.INFO.Printf("Sending a message successfully, the messageId is %v", *result.MessageId)
	return nil

}

// consume is a method which keeps consuming deliveries from a channel, until there is an error or a stop signal
func (b *Broker) consume(deliveries <-chan *sqs.ReceiveMessageOutput, concurrency int, taskProcessor iface.TaskProcessor, pool chan struct{}) error {

	errorsChan := make(chan error)

	for {
		whetherContinue, err := b.consumeDeliveries(deliveries, concurrency, taskProcessor, pool, errorsChan)
		if err != nil {
			return err
		}
		if whetherContinue == false {
			return nil
		}
	}
}

// consumeOne is a method consumes a delivery. If a delivery was consumed successfully, it will be deleted from AWS SQS
func (b *Broker) consumeOne(delivery *sqs.ReceiveMessageOutput, taskProcessor iface.TaskProcessor) error {
	if len(delivery.Messages) == 0 {
		log.ERROR.Printf("received an empty message, the delivery was %v", delivery)
		return errors.New("received empty message, the delivery is " + awsutil.Prettify(delivery))
	}

	if b.GetConfig().SQS.VisibilityHeartBeat {
		notify := make(chan struct{})
		defer close(notify)

		b.visibilityHeartbeat(delivery, notify)
	}

	if b.GetConfig().SQS.VisibilityHeartBeat {
		notify := make(chan struct{})
		defer close(notify)

		b.visibilityHeartbeat(delivery, notify)
	}

	sig := new(tasks.Signature)
	decoder := json.NewDecoder(strings.NewReader(*delivery.Messages[0].Body))
	decoder.UseNumber()
	if err := decoder.Decode(sig); err != nil {
		log.ERROR.Printf("unmarshal error. the delivery is %v", delivery)
		// if the unmarshal fails, remove the delivery from the queue
		if delErr := b.deleteOne(delivery); delErr != nil {
			log.ERROR.Printf("error when deleting the delivery. delivery is %v, Error=%s", delivery, delErr)
		}
		return err
	}
	if delivery.Messages[0].ReceiptHandle != nil {
		sig.SQSReceiptHandle = *delivery.Messages[0].ReceiptHandle
	}

	// If the task is not registered return an error
	// and leave the message in the queue
	if !b.IsTaskRegistered(sig.Name) {
		if sig.IgnoreWhenTaskNotRegistered {
			if err := b.deleteOne(delivery); err != nil {
				log.ERROR.Printf("error when deleting the delivery. delivery is %v, Error=%s", delivery, err)
			}
		}
		return fmt.Errorf("task %s is not registered", sig.Name)
	}

	err := taskProcessor.Process(sig)
	if err != nil {
		// stop task deletion in case we want to send messages to dlq in sqs
		if errors.Is(err, errs.ErrStopTaskDeletion) {
			return nil
		}
		return err
	}
	// Delete message after successfully consuming and processing the message
	if err = b.deleteOne(delivery); err != nil {
		log.ERROR.Printf("error when deleting the delivery. delivery is %v, Error=%s", delivery, err)
	}
	return err
}

// deleteOne is a method delete a delivery from AWS SQS
func (b *Broker) deleteOne(delivery *sqs.ReceiveMessageOutput) error {
	qURL := b.defaultQueueURL()
	_, err := b.service.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
		QueueUrl:      qURL,
		ReceiptHandle: delivery.Messages[0].ReceiptHandle,
	})

	if err != nil {
		return err
	}
	return nil
}

// defaultQueueURL is a method returns the default queue url
func (b *Broker) defaultQueueURL() *string {
	if b.queueUrl != nil {
		return b.queueUrl
	} else {
		return aws.String(b.GetConfig().Broker + "/" + b.GetConfig().DefaultQueue)
	}

}

// receiveMessage is a method receives a message from specified queue url
func (b *Broker) receiveMessage(qURL *string) (*sqs.ReceiveMessageOutput, error) {
	var waitTimeSeconds int
	var visibilityTimeout *int
	if b.GetConfig().SQS != nil {
		waitTimeSeconds = b.GetConfig().SQS.WaitTimeSeconds
		visibilityTimeout = b.GetConfig().SQS.VisibilityTimeout
	}

	input := &sqs.ReceiveMessageInput{
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameSentTimestamp,
		},
		MessageAttributeNames: []string{
			string(types.QueueAttributeNameAll),
		},
		QueueUrl:            qURL,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     int32(waitTimeSeconds),
	}
	if visibilityTimeout != nil {
		input.VisibilityTimeout = int32(*visibilityTimeout)
	}
	result, err := b.service.ReceiveMessage(context.TODO(), input)
	if err != nil {
		return nil, err
	}
	return result, err
}

// initializePool is a method which initializes concurrency pool
func (b *Broker) initializePool(pool chan struct{}, concurrency int) {
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}
}

// consumeDeliveries is a method consuming deliveries from deliveries channel
func (b *Broker) consumeDeliveries(deliveries <-chan *sqs.ReceiveMessageOutput, concurrency int, taskProcessor iface.TaskProcessor, pool chan struct{}, errorsChan chan error) (bool, error) {
	select {
	case err := <-errorsChan:
		return false, err
	case d := <-deliveries:

		b.processingWG.Add(1)

		// Consume the task inside a goroutine so multiple tasks
		// can be processed concurrently
		go func() {

			if err := b.consumeOne(d, taskProcessor); err != nil {
				errorsChan <- err
			}

			b.processingWG.Done()

			if concurrency > 0 {
				// give worker back to pool
				pool <- struct{}{}
			}
		}()
	case <-b.GetStopChan():
		return false, nil
	}
	return true, nil
}

// continueReceivingMessages is a method returns a continue signal
func (b *Broker) continueReceivingMessages(qURL *string, deliveries chan *sqs.ReceiveMessageOutput) (bool, error) {
	select {
	// A way to stop this goroutine from b.StopConsuming
	case <-b.stopReceivingChan:
		return false, nil
	default:
		output, err := b.receiveMessage(qURL)
		if err != nil {
			return true, err
		}
		if len(output.Messages) == 0 {
			return true, nil
		}
		go func() { deliveries <- output }()
	}
	return true, nil
}

// visibilityHeartbeat is a method that sends a heartbeat signal to AWS SQS to keep a message invisible to other consumers while being processed.
func (b *Broker) visibilityHeartbeat(delivery *sqs.ReceiveMessageOutput, notify <-chan struct{}) {
	if b.GetConfig().SQS.VisibilityTimeout == nil || *b.GetConfig().SQS.VisibilityTimeout == 0 {
		return
	}

	ticker := time.NewTicker(time.Duration(*b.GetConfig().SQS.VisibilityTimeout) * 500 * time.Millisecond)

	go func() {
		for {
			select {
			case <-notify:
				ticker.Stop()

				return
			case <-b.stopReceivingChan:
				ticker.Stop()

				return
			case <-ticker.C:
				// Extend the delivery visibility timeout
				_, err := b.service.ChangeMessageVisibility(context.TODO(), &sqs.ChangeMessageVisibilityInput{
					QueueUrl:          b.defaultQueueURL(),
					ReceiptHandle:     delivery.Messages[0].ReceiptHandle,
					VisibilityTimeout: int32(*b.GetConfig().SQS.VisibilityTimeout),
				})
				if err != nil {
					log.ERROR.Printf("Error when changing delivery visibility: %v", err)
				}
			}
		}
	}()
}

// stopReceiving is a method sending a signal to stopReceivingChan
func (b *Broker) stopReceiving() {
	// Stop the receiving goroutine
	b.stopReceivingChan <- 1
}

// getQueueURL is a method returns that returns queueURL first by checking if custom queue was set and usign it
// otherwise using default queueName from config
func (b *Broker) getQueueURL(taskProcessor iface.TaskProcessor) *string {
	queueName := b.GetConfig().DefaultQueue
	if taskProcessor.CustomQueue() != "" {
		queueName = taskProcessor.CustomQueue()
	}

	return aws.String(b.GetConfig().Broker + "/" + queueName)
}
