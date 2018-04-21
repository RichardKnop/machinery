package brokers

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

// AWSSQSBroker represents a AWS SQS broker
// There are examples on: https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/sqs-example-create-queue.html
type AWSSQSBroker struct {
	Broker
	processingWG      sync.WaitGroup // use wait group to make sure task processing completes on interrupt signal
	receivingWG       sync.WaitGroup
	stopReceivingChan chan int
	sess              *session.Session
	service           sqsiface.SQSAPI
}

// NewAWSSQSBroker creates new Broker instance
func NewAWSSQSBroker(cnf *config.Config) Interface {
	b := &AWSSQSBroker{Broker: New(cnf)}
	if cnf.SQS != nil && cnf.SQS.Client != nil {
		// Use provided *SQS client
		b.service = cnf.SQS.Client
	} else {
		// Initialize a session that the SDK will use to load credentials from the shared credentials file, ~/.aws/credentials.
		// See details on: https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html
		// Also, env AWS_REGION is also required
		b.sess = session.Must(session.NewSessionWithOptions(session.Options{
			SharedConfigState: session.SharedConfigEnable,
		}))
		b.service = sqs.New(b.sess)
	}

	return b
}

// GetPendingTasks returns a slice of task.Signatures waiting in the queue
func (b *AWSSQSBroker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	return nil, errors.New("Not implemented")
}

// StartConsuming enters a loop and waits for incoming messages
func (b *AWSSQSBroker) StartConsuming(consumerTag string, concurrency int, taskProcessor TaskProcessor) (bool, error) {
	b.startConsuming(consumerTag, taskProcessor)
	qURL := b.defaultQueueURL()
	deliveries := make(chan *sqs.ReceiveMessageOutput)

	b.stopReceivingChan = make(chan int)
	b.receivingWG.Add(1)

	go func() {
		defer b.receivingWG.Done()

		log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.stopReceivingChan:
				return
			default:
				output, err := b.receiveMessage(qURL)
				if err != nil {
					log.ERROR.Printf("Queue consume error: %s", err)
					continue
				}
				if len(output.Messages) == 0 {
					continue
				}

				deliveries <- output
			}

			whetherContinue, err := b.continueReceivingMessages(qURL, deliveries)
			if err != nil {
				log.ERROR.Printf("Error when receiving messages. Error: %v", err)
			}
			if whetherContinue == false {
				return
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor); err != nil {
		return b.retry, err
	}

	return b.retry, nil
}

// StopConsuming quits the loop
func (b *AWSSQSBroker) StopConsuming() {
	b.stopConsuming()

	b.stopReceiving()

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	// Waiting for the receiving goroutine to have stopped
	b.receivingWG.Wait()
}

// Publish places a new message on the default queue
func (b *AWSSQSBroker) Publish(signature *tasks.Signature) error {

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// Check that signature.RoutingKey is set, if not switch to DefaultQueue
	AdjustRoutingKey(b, signature)

	MsgInput := &sqs.SendMessageInput{
		MessageBody: aws.String(string(msg)),
		QueueUrl:    aws.String(b.cnf.Broker + "/" + signature.RoutingKey),
	}

	// if this is a fifo queue, there needs to be some additional parameters.
	if strings.HasSuffix(signature.RoutingKey, ".fifo") {
		// Use Machinery's signature Task UUID as SQS Message Group ID.
		MsgDedupID := signature.UUID
		MsgInput.MessageDeduplicationId = aws.String(MsgDedupID)

		// Use Machinery's signature Group UUID as SQS Message Group ID.
		MsgGroupID := signature.GroupUUID
		MsgInput.MessageGroupId = aws.String(MsgGroupID)
	}

	// Check the ETA signature field, if it is set and it is in the future,
	// and is not a fifo queue, set a delay in seconds for the task.
	if signature.ETA != nil && !strings.HasSuffix(signature.RoutingKey, ".fifo") {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			MsgInput.DelaySeconds = aws.Int64(signature.ETA.Unix() - now.Unix())
		}
	}

	result, err := b.service.SendMessage(MsgInput)

	if err != nil {
		log.ERROR.Printf("Error when sending a message: %v", err)
		return err

	}
	log.INFO.Printf("Sending a message successfully, the messageId is %v", *result.MessageId)
	return nil

}

// consume is a method which keeps consuming deliveries from a channel, until there is an error or a stop signal
func (b *AWSSQSBroker) consume(deliveries <-chan *sqs.ReceiveMessageOutput, concurrency int, taskProcessor TaskProcessor) error {
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	go func() {
		b.initializePool(pool, concurrency)
	}()

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
func (b *AWSSQSBroker) consumeOne(delivery *sqs.ReceiveMessageOutput, taskProcessor TaskProcessor) error {
	if len(delivery.Messages) == 0 {
		log.ERROR.Printf("received an empty message, the delivery was %v", delivery)
		return errors.New("received empty message, the delivery is " + delivery.GoString())
	}

	sig := new(tasks.Signature)
	decoder := json.NewDecoder(strings.NewReader(*delivery.Messages[0].Body))
	decoder.UseNumber()
	if err := decoder.Decode(sig); err != nil {
		log.ERROR.Printf("unmarshal error. the delivery is %v", delivery)
		return err
	}

	// If the task is not registered return an error
	// and leave the message in the queue
	if !b.IsTaskRegistered(sig.Name) {
		return fmt.Errorf("task %s is not registered", sig.Name)
	}

	err := taskProcessor.Process(sig)
	if err != nil {
		// Delete message after successfully consuming and processing the message
		if err := b.deleteOne(delivery); err != nil {
			log.ERROR.Printf("error when deleting the delivery. the delivery is %v", delivery)
		}
	}
	return err
}

// deleteOne is a method delete a delivery from AWS SQS
func (b *AWSSQSBroker) deleteOne(delivery *sqs.ReceiveMessageOutput) error {
	qURL := b.defaultQueueURL()
	_, err := b.service.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      qURL,
		ReceiptHandle: delivery.Messages[0].ReceiptHandle,
	})

	if err != nil {
		return err
	}
	return nil
}

// defaultQueueURL is a method returns the default queue url
func (b *AWSSQSBroker) defaultQueueURL() *string {
	return aws.String(b.cnf.Broker + "/" + b.cnf.DefaultQueue)
}

// receiveMessage is a method receives a message from specified queue url
func (b *AWSSQSBroker) receiveMessage(qURL *string) (*sqs.ReceiveMessageOutput, error) {
	var waitTimeSeconds int
	if b.cnf.SQS != nil {
		waitTimeSeconds = b.cnf.SQS.WaitTimeSeconds
	} else {
		waitTimeSeconds = 0
	}
	result, err := b.service.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            qURL,
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:   aws.Int64(int64(b.cnf.ResultsExpireIn)), // 10 hours
		WaitTimeSeconds:     aws.Int64(int64(waitTimeSeconds)),
	})
	if err != nil {
		return nil, err
	}
	return result, err
}

// initializePool is a method which initializes concurrency pool
func (b *AWSSQSBroker) initializePool(pool chan struct{}, concurrency int) {
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}
}

// consumeDeliveries is a method consuming deliveries from deliveries channel
func (b *AWSSQSBroker) consumeDeliveries(deliveries <-chan *sqs.ReceiveMessageOutput, concurrency int, taskProcessor TaskProcessor, pool chan struct{}, errorsChan chan error) (bool, error) {
	select {
	case err := <-errorsChan:
		return false, err
	case d := <-deliveries:
		if concurrency > 0 {
			// get worker from pool (blocks until one is available)
			<-pool
		}

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
	case <-b.stopChan:
		return false, nil
	}
	return true, nil
}

// continueReceivingMessages is a method returns a continue signal
func (b *AWSSQSBroker) continueReceivingMessages(qURL *string, deliveries chan *sqs.ReceiveMessageOutput) (bool, error) {
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

// stopReceiving is a method sending a signal to stopReceivingChan
func (b *AWSSQSBroker) stopReceiving() {
	// Stop the receiving goroutine
	b.stopReceivingChan <- 1
}
