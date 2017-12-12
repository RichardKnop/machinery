package brokers

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"sync"
)

// AWSSQSBroker represents a AWS SQS broker
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
	b.sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	b.service = sqs.New(b.sess)

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
	b.AdjustRoutingKey(signature)

	MsgInput := &sqs.SendMessageInput{
		MessageBody: aws.String(string(msg)),
		QueueUrl:    aws.String(b.cnf.Broker + "/" + signature.RoutingKey),
	}

	// if this is a fifo queue, there needs to be some addtional parameters.
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
		log.INFO.Println("Error", err)
		return err

	}
	log.INFO.Println("Success", *result.MessageId)
	return nil

}

// TODO: Add GetPendingTasks() & add AssignWorker(), refer to
// RichardKnop's broker.

func (b *AWSSQSBroker) consume(deliveries <-chan *sqs.ReceiveMessageOutput, concurrency int, taskProcessor TaskProcessor) error {
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	go func() {
		b.initializePool(pool, concurrency)
	}()

	errorsChan := make(chan error)

	for {
		err := b.consumeDeliveries(deliveries, concurrency, taskProcessor, pool, errorsChan)
		if err != nil {
			return err
		}
	}
}

func (b *AWSSQSBroker) consumeOne(delivery *sqs.ReceiveMessageOutput, taskProcessor TaskProcessor) error {
	sig := new(tasks.Signature)
	if len(delivery.Messages) == 0 {
		log.ERROR.Printf("received an empty message, the delivery was %v", delivery)
		return errors.New("received empty message, the delivery is " + delivery.GoString())
	}
	msg := delivery.Messages[0].Body
	if err := json.Unmarshal([]byte(*msg), sig); err != nil {
		log.ERROR.Printf("unmarshal error. the delivery is %v", delivery)
		return err
	}

	// Delete message after consuming successfully
	err := b.deleteOne(delivery)
	if err != nil {
		log.ERROR.Printf("error when deleting the delivery. the delivery is %v", delivery)
	}

	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(sig.Name) {
		err := b.Publish(sig)
		if err != nil {
			return err
		}
		log.INFO.Printf("requeue a task to default queue: %v", sig)
		return nil
	}
	return taskProcessor.Process(sig)
}

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

func (b *AWSSQSBroker) defaultQueueURL() *string {
	return aws.String(b.cnf.Broker + "/" + b.cnf.DefaultQueue)
}

func (b *AWSSQSBroker) receiveMessage(qURL *string) (*sqs.ReceiveMessageOutput, error) {
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
		WaitTimeSeconds:     aws.Int64(0),
	})
	if err != nil {
		return nil, err
	}
	return result, err
}

func (b *AWSSQSBroker) initializePool(pool chan struct{}, concurrency int) {
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}
}

func (b *AWSSQSBroker) consumeDeliveries(deliveries <-chan *sqs.ReceiveMessageOutput, concurrency int, taskProcessor TaskProcessor, pool chan struct{}, errorsChan chan error) error {
	select {
	case err := <-errorsChan:
		return err
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
		return nil
	}
	return nil
}

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

func (b *AWSSQSBroker) stopReceiving() {
	// Stop the receiving goroutine
	b.stopReceivingChan <- 1
}
