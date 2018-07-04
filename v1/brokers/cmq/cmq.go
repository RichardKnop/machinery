package cmq

import (
	"github.com/RichardKnop/machinery/v1/common"
	"sync"
	"github.com/baocaixiong/cmq-golang-sdk"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/baocaixiong/cmq-golang-sdk/models"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"strings"
	"fmt"
	"errors"
	"encoding/json"
	"time"
)

const (
	maxCMQDelay = time.Minute * 60
)

type TopicSupport interface {
	TopicPublish(string, *tasks.Signature, ...string) error
}

type Broker struct {
	common.Broker
	processingWG      sync.WaitGroup
	receivingWG       sync.WaitGroup
	stopReceivingChan chan int
	client            *cmq.Client
}

func New(cnf *config.Config, opt *cmq.Options) iface.Broker {
	b := &Broker{
		Broker: common.NewBroker(cnf),
	}

	if cnf.CMQ != nil && cnf.CMQ.Client != nil {
		b.client = cnf.CMQ.Client
	} else {
		b.client = cmq.NewClient(
			cmq.Region(opt.Region),
			cmq.NetEnv(opt.NetEnv),
			cmq.SetCredential(opt.Credential),
		)
	}

	return b
}

func (b *Broker) StartConsuming(consumerTag string, concurrency int, p iface.TaskProcessor) (bool, error) {
	b.Broker.StartConsuming(consumerTag, concurrency, p)
	b.stopReceivingChan = make(chan int)
	deliveries := make(chan *models.ReceiveMessageResp)
	b.receivingWG.Add(1)
	log.INFO.Print("[*] Waiting for message. To exit press CTRL+C")

	go func() {
		defer b.receivingWG.Done()

		for {
			select {
			case <-b.stopReceivingChan:
				return
			default:
				output, err := b.receiveMessage()
				if err != nil {
					log.ERROR.Printf("Queue consume error: %s", err)
					continue
				}
				if output == nil {
					continue
				}

				deliveries <- output
			}

			whetherContinue, err := b.continueReceivingMessages(deliveries)
			if err != nil {
				log.ERROR.Printf("Error when receiving messages. Error: %v", err)
			}
			if whetherContinue == false {
				return
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, p); err != nil {
		return b.GetRetry(), err
	}

	return b.GetRetry(), nil

	return true, nil
}

// StopConsuming quits the loop
func (b *Broker) StopConsuming() {
	log.INFO.Printf("receive stop signal")
	b.Broker.StopConsuming()

	b.stopReceiving()

	// Waiting for any tasks being processed to finish
	log.INFO.Printf("waiting processing waitGroup done")
	b.processingWG.Wait()

	log.INFO.Printf("waiting receiging waitGroup done")
	// Waiting for the receiving goroutine to have stopped
	b.receivingWG.Wait()
}

func (b *Broker) Publish(signature *tasks.Signature) error {
	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	b.AdjustRoutingKey(signature)

	input := models.NewSendMessageReq(b.GetConfig().DefaultQueue, string(msg))

	if signature.ETA != nil {
		now := time.Now().UTC()
		delay := signature.ETA.Sub(now)
		if delay > 0 {
			if delay > maxCMQDelay {
				return errors.New("max cmq delay exceeded")
			}
			input.DelaySeconds = cmq.IntPtr(int(delay.Seconds()))
		}
	}

	output := models.NewSendMessageResp()
	err = b.client.Send(input, output)
	if err != nil {
		log.ERROR.Printf("Error when sending a message: %v", err)
		return err

	}
	log.INFO.Printf("Sending a message successfully, the messageId is %v", output.MsgId)
	return nil
}

func (b *Broker) TopicPublish(topic string, signature *tasks.Signature, msgTags ...string) error {
	if len(signature.RoutingKey) == 0 {
		return fmt.Errorf("msg RoutingKey can not empty")
	}

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}
	input := models.NewPublishMessageReq(topic, string(msg))

	input.RoutingKey = cmq.StringPtr(signature.RoutingKey)
	if len(msgTags) > 0 {
		input.MsgTag = cmq.SliceStringPtr(msgTags)
	}
	output := models.NewPublishMessageResp()
	err = b.client.Send(input, output)
	if err != nil {
		log.ERROR.Printf("Error when sending a message to topic: %v", err)
		return err
	}

	log.INFO.Printf("Sending a message to topic successfully, the messageId is %v", output.MsgId)
	return nil
}

func (b *Broker) consume(deliveries chan *models.ReceiveMessageResp, concurrency int, processor iface.TaskProcessor) error {
	pool := make(chan struct{}, concurrency)

	go func() {
		b.initializePool(pool, concurrency)
	}()

	errorsChan := make(chan error)
	for {
		whetherContinue, err := b.consumeDeliveries(deliveries, concurrency, processor, pool, errorsChan)
		if err != nil {
			return err
		}
		if whetherContinue == false {
			return nil
		}
	}
}

func (b *Broker) consumeOne(delivery *models.ReceiveMessageResp, taskProcessor iface.TaskProcessor) error {
	sig := new(tasks.Signature)
	decoder := json.NewDecoder(strings.NewReader(delivery.MsgBody))
	decoder.UseNumber()
	if err := decoder.Decode(sig); err != nil {
		if e := b.deleteOne(delivery); e != nil {
			log.ERROR.Printf("unmarshal error. the delivery is %v, delete failed:%v", delivery, e)
		}
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
		return err
	}
	// Delete message after successfully consuming and processing the message
	if err = b.deleteOne(delivery); err != nil {
		log.ERROR.Printf("error when deleting the delivery. the delivery is %v", delivery)
	}
	return err
}

func (b *Broker) consumeDeliveries(deliveries <-chan *models.ReceiveMessageResp, concurrency int,
	taskProcessor iface.TaskProcessor, pool chan struct{}, errorsChan chan error) (bool, error) {
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
	case <-b.GetStopChan():
		log.INFO.Printf("receive stop signal")
		return false, nil
	}
	return true, nil
}
func (b *Broker) deleteOne(delivery *models.ReceiveMessageResp) error {
	input := models.NewDeleteMessageReq(b.GetConfig().DefaultQueue, delivery.ReceiptHandle)
	output := models.NewDeleteMessageResp()
	err := b.client.Send(input, output)
	if err != nil {
		return err
	}
	return nil
}

// initializePool is a method which initializes concurrency pool
func (b *Broker) initializePool(pool chan struct{}, concurrency int) {
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}
}

func (b *Broker) receiveMessage(waitTimeSecondsSlice ...int) (*models.ReceiveMessageResp, error) {
	var waitTimeSeconds int
	if len(waitTimeSecondsSlice) > 0 {
		waitTimeSeconds = waitTimeSecondsSlice[0]
	} else if b.GetConfig().CMQ != nil {
		waitTimeSeconds = b.GetConfig().CMQ.WaitTimeSeconds
	} else {
		waitTimeSeconds = 0
	}

	input := models.NewReceiveMessageReq(b.GetConfig().DefaultQueue)
	input.PollingWaitSeconds = cmq.IntPtr(waitTimeSeconds)
	output := models.NewReceiveMessageResp()
	err := b.client.Send(input, output)
	if err != nil {
		return nil, err
	}

	if output.Code == 7000 { // 没有新消息
		log.INFO.Printf("receive 0 messages, requestId:%s", output.RequestId)
		return nil, nil
	}

	return output, nil
}

// continueReceivingMessages is a method returns a continue signal
func (b *Broker) continueReceivingMessages(deliveries chan *models.ReceiveMessageResp) (bool, error) {
	select {
	// A way to stop this goroutine from b.StopConsuming
	case <-b.stopReceivingChan:
		return false, nil
	default:
		output, err := b.receiveMessage(0)
		if err != nil {
			return true, err
		}
		if output != nil {
			go func() { deliveries <- output }()
		}
	}
	return true, nil
}

// stopReceiving is a method sending a signal to stopReceivingChan
func (b *Broker) stopReceiving() {
	// Stop the receiving goroutine
	b.stopReceivingChan <- 1
}
