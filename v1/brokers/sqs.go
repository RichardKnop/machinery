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
	"sync"
)

// SQSBroker represents a SQS broker
type SQSBroker struct {
	Broker
	processingWG      sync.WaitGroup // use wait group to make sure task processing completes on interrupt signal
	receivingWG       sync.WaitGroup
	stopReceivingChan chan int
	sess              *session.Session
}

// SQSNew creates new Broker instance
func NewSQSBroker(cnf *config.Config) Interface {

	b := &SQSBroker{Broker: New(cnf)}
	b.sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	return b
}

// SetRegisteredTaskNames sets registered task names
func (b *SQSBroker) SetRegisteredTaskNames(names []string) {
	b.registeredTaskNames = names
}

// IsTaskRegistered returns true if the task is registered with this broker
func (b *SQSBroker) IsTaskRegistered(name string) bool {
	for _, registeredTaskName := range b.registeredTaskNames {
		if registeredTaskName == name {
			return true
		}
	}
	return false
}

// GetPendingTasks returns a slice of task.Signatures waiting in the queue
func (b *SQSBroker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	return nil, errors.New("Not implemented")
}

// StartConsuming enters a loop and waits for incoming messages
func (b *SQSBroker) StartConsuming(consumerTag string, concurrency int, taskProcessor TaskProcessor) (bool, error) {
	b.startConsuming(consumerTag, taskProcessor)
	// TODO: decide for default queue or selected queue
	qURL := b.cnf.Broker + "/" + b.cnf.DefaultQueue
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
				svc := sqs.New(b.sess)
				result, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
					AttributeNames: []*string{
						aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
					},
					MessageAttributeNames: []*string{
						aws.String(sqs.QueueAttributeNameAll),
					},
					QueueUrl:            &qURL,
					MaxNumberOfMessages: aws.Int64(1),
					VisibilityTimeout:   aws.Int64(36000), // 10 hours
					WaitTimeSeconds:     aws.Int64(0),
				})
				if err != nil {
					//TODO: Elegant error handling
					log.ERROR.Printf("Queue consume error: %s", err)
					continue
				}
				// There is no message
				if len(result.Messages) == 0 {
					//fmt.Println("Received no messages")
					continue
				}

				deliveries <- result
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor); err != nil {
		return b.retry, err
	}

	return b.retry, nil
}

// StopConsuming quits the loop
func (b *SQSBroker) StopConsuming() {
	b.stopConsuming()
	// Stop the receiving goroutine
	b.stopReceivingChan <- 1

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	// Waiting for the receiving goroutine to have stopped
	b.receivingWG.Wait()
}

// Publish places a new message on the default queue
func (b *SQSBroker) Publish(signature *tasks.Signature) error {

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// Check that signature.RoutingKey is set, if not switch to DefaultQueue
	b.AdjustRoutingKey(signature)

	// Create a SQS service client.
	svc := sqs.New(b.sess)

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

	result, err := svc.SendMessage(MsgInput)

	if err == nil {
		fmt.Println("Success", *result.MessageId)
	}

	fmt.Println("Error", err)
	return err
}

// TODO: Add GetPendingTasks() & add AssignWorker(), refer to
// RichardKnop's broker.

func (b *SQSBroker) consume(deliveries <-chan *sqs.ReceiveMessageOutput, concurrency int, taskProcessor TaskProcessor) error {
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	errorsChan := make(chan error)

	for {
		select {
		case err := <-errorsChan:
			return err
		case d := <-deliveries:
			if concurrency > 0 {
				// get worker from pool (blocks until one is available)
				<-pool
			}

			b.processingWG.Add(1)

			// Consume the task inside a gotourine so multiple tasks
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
		default:
			//fmt.Println("no message received")
		}

	}
}

// TODO: delete msg after consuming
func (b *SQSBroker) consumeOne(delivery *sqs.ReceiveMessageOutput, taskProcessor TaskProcessor) error {
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

	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(sig.Name) {
		//todo: publish it to queue
		err := b.Publish(sig)
		if err != nil {
			return err
		}
		log.INFO.Printf("requeue a task to default queue: %v", sig)
		return nil
	}
	return taskProcessor.Process(sig)
}
