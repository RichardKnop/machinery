/*
 * Tencent is pleased to support the open source community by making Blueking Container Service available.
 * Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
 * Licensed under the MIT License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://opensource.org/licenses/MIT
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package etcd is broker use etcd
package etcd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/RichardKnop/machinery/v2/brokers/errs"
	"github.com/RichardKnop/machinery/v2/brokers/iface"
	"github.com/RichardKnop/machinery/v2/common"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/log"
	"github.com/RichardKnop/machinery/v2/tasks"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"
)

const (
	pendingTaskPrefix  = "/machinery/v2/broker/pending_tasks"
	runningTaskPrefix  = "/machinery/v2/broker/running_tasks"
	delayedTaskPrefix  = "/machinery/v2/broker/delayed_tasks"
	delayedTaskLockKey = "/machinery/v2/lock/delayed_tasks"
)

type etcdBroker struct {
	common.Broker
	ctx         context.Context
	client      *clientv3.Client
	wg          sync.WaitGroup
	pendingTask map[string]struct{}
	runningTask map[string]struct{}
	delayedTask map[string]*delayTask
	mtx         sync.RWMutex
	delayedMtx  sync.RWMutex
}

// New ..
func New(ctx context.Context, conf *config.Config) (iface.Broker, error) {
	etcdConf := clientv3.Config{
		Endpoints:   []string{conf.Broker},
		Context:     ctx,
		DialTimeout: time.Second * 5,
		TLS:         conf.TLSConfig,
	}

	client, err := clientv3.New(etcdConf)
	if err != nil {
		return nil, err
	}

	broker := etcdBroker{
		Broker:      common.NewBroker(conf),
		ctx:         ctx,
		client:      client,
		pendingTask: make(map[string]struct{}),
		runningTask: make(map[string]struct{}),
		delayedTask: make(map[string]*delayTask),
	}

	return &broker, nil

}

// nolint
// StartConsuming ...
func (b *etcdBroker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	if concurrency < 1 {
		concurrency = runtime.NumCPU()
	}
	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)

	log.INFO.Printf("[*] Waiting for messages, concurrency=%d. To exit press CTRL+C", concurrency)

	// Channel to which we will push tasks ready for processing by worker
	deliveries := make(chan Delivery)
	defer log.INFO.Printf("stop all consuming and handle done")
	defer b.wg.Wait()

	ctx, cancel := context.WithCancel(b.ctx)
	defer cancel()

	// list watch running task
	b.wg.Add(1)
	go func() {
		defer func() {
			cancel()
			b.wg.Done()
			log.INFO.Printf("list watch running task stopped")
		}()

		for {
			select {
			case <-b.GetStopChan():
				return
			case <-ctx.Done():
				return
			default:
				err := b.listWatchRunningTask(ctx)
				if err != nil {
					log.ERROR.Printf("list watch running task failed, err: %s", err)
					time.Sleep(time.Second)
				}
			}
		}
	}()

	// list watch pending task
	b.wg.Add(1)
	go func() {
		defer func() {
			cancel()
			b.wg.Done()
			log.INFO.Printf("list watch pending task stopped")
		}()

		queue := getQueue(b.GetConfig(), taskProcessor)
		for {
			select {
			case <-b.GetStopChan():
				return
			case <-ctx.Done():
				return
			default:
				err := b.listWatchPendingTask(ctx, queue)
				if err != nil {
					log.ERROR.Printf("list watch pending task failed, err: %s", err)
					time.Sleep(time.Second)
				}
			}
		}
	}()

	// A receiving goroutine keeps popping messages from the queue by BLPOP
	// If the message is valid and can be unmarshaled into a proper structure
	// we send it to the deliveries channel
	b.wg.Add(1)
	go func() {
		defer func() {
			cancel()
			close(deliveries)
			b.wg.Done()
			log.INFO.Printf("handle next task stopped")
		}()

		for {
			select {
			case <-b.GetStopChan():
				return
			case <-ctx.Done():
				return
			default:
				if !taskProcessor.PreConsumeHandler() {
					continue
				}

				task := b.nextTask(ctx, getQueue(b.GetConfig(), taskProcessor), consumerTag)
				if task == nil {
					time.Sleep(time.Second)
					continue
				}

				deliveries <- task
			}
		}
	}()

	// A goroutine to watch for delayed tasks and push them to deliveries
	// channel for consumption by the worker
	b.wg.Add(1)
	go func() {
		defer func() {
			cancel()
			b.wg.Done()
			log.INFO.Printf("handle delayed task stopped")
		}()

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				return
			case <-ctx.Done():
				return
			default:
				err := b.handleDelayedTask(ctx)
				if err != nil && !errors.Is(err, context.DeadlineExceeded) {
					log.ERROR.Printf("handle delayed task failed, err: %s", err)
				}
				time.Sleep(time.Second)
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor); err != nil {
		log.WARNING.Printf("consume stopped, err=%v, retry=%t", err, b.GetRetry())
		return b.GetRetry(), err
	}

	log.INFO.Printf("consume stopped, retry=%t", b.GetRetry())
	return b.GetRetry(), nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *etcdBroker) consume(deliveries <-chan Delivery, concurrency int, taskProcessor iface.TaskProcessor) error {
	eg, ctx := errgroup.WithContext(b.ctx)

	for i := 0; i < concurrency; i++ {
		eg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()

				case t, ok := <-deliveries:
					if !ok {
						return nil
					}

					if err := b.consumeOne(t, taskProcessor); err != nil {
						return err
					}
				}
			}
		})
	}

	return eg.Wait()
}

// consumeOne processes a single message using TaskProcessor
func (b *etcdBroker) consumeOne(delivery Delivery, taskProcessor iface.TaskProcessor) error {
	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(delivery.Signature().Name) {
		log.INFO.Printf("Task not registered with this worker. Requeuing message: %s", delivery.Body())

		if !delivery.Signature().IgnoreWhenTaskNotRegistered {
			delivery.Nack()
		}
		return nil
	}

	log.DEBUG.Printf("Received new message: %s", delivery.Body())
	defer delivery.Ack()

	return taskProcessor.Process(delivery.Signature())
}

// StopConsuming 停止
func (b *etcdBroker) StopConsuming() {
	b.Broker.StopConsuming()

	b.wg.Wait()
}

// Publish put kvs to etcd stor
func (b *etcdBroker) Publish(ctx context.Context, signature *tasks.Signature) error {
	// Adjust routing key (this decides which queue the message will be published to)
	b.Broker.AdjustRoutingKey(signature)

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	key := fmt.Sprintf("%s/%s/%s", pendingTaskPrefix, signature.RoutingKey, signature.UUID)

	// Check the ETA signature field, alway delay the task if not nil,
	// prevent the key overwrite by slow ack request
	if signature.ETA != nil {
		key = fmt.Sprintf("%s/eta-%d/%s/%s",
			delayedTaskPrefix, signature.ETA.UnixMilli(), signature.RoutingKey, signature.UUID)
	}

	_, err = b.client.Put(ctx, key, string(msg))
	if err != nil {
		log.ERROR.Printf("Publish queue[%s] new message: %s", key, string(msg))
	} else {
		log.DEBUG.Printf("Publish queue[%s] new message: %s", key, string(msg))
	}
	return err
}

func (b *etcdBroker) getTasks(ctx context.Context, key string) ([]*tasks.Signature, error) {
	resp, err := b.client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	result := make([]*tasks.Signature, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(bytes.NewReader(kv.Value))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, errs.NewErrCouldNotUnmarshalTaskSignature(kv.Value, err)
		}

		result = append(result, signature)
	}

	return result, nil
}

// GetPendingTasks 获取执行队列, 任务统计可使用
func (b *etcdBroker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	if queue == "" {
		queue = b.GetConfig().DefaultQueue
	}

	key := fmt.Sprintf("%s/%s", pendingTaskPrefix, queue)
	items, err := b.getTasks(b.ctx, key)
	if err != nil {
		return nil, err
	}

	return items, nil
}

// GetDelayedTasks 任务统计可使用
func (b *etcdBroker) GetDelayedTasks() ([]*tasks.Signature, error) {
	items, err := b.getTasks(b.ctx, delayedTaskPrefix)
	if err != nil {
		return nil, err
	}

	return items, nil
}

func (b *etcdBroker) nextTask(ctx context.Context, queue string, consumerTag string) Delivery {
	b.mtx.Lock()
	runningTask := make(map[string]struct{}, len(b.runningTask))
	for k, v := range b.runningTask {
		runningTask[k] = v
	}
	pendingTask := make(map[string]struct{}, len(b.pendingTask))
	for k, v := range b.pendingTask {
		pendingTask[k] = v
	}
	b.mtx.Unlock()

	for k := range pendingTask {
		if !strings.Contains(k, queue) {
			continue
		}

		if _, ok := runningTask[k]; ok {
			continue
		}

		d, err := NewDelivery(ctx, b.client, k, consumerTag)
		if err != nil {
			continue
		}

		b.mtx.Lock()
		b.runningTask[k] = struct{}{}
		b.mtx.Unlock()

		return d
	}

	return nil
}

func (b *etcdBroker) listWatchRunningTask(ctx context.Context) error {
	// List
	listCtx, listCancel := context.WithTimeout(ctx, time.Second*10)
	defer listCancel()
	resp, err := b.client.Get(listCtx, runningTaskPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return err
	}

	b.mtx.Lock()
	b.runningTask = make(map[string]struct{})
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		taskKey := findTaskKey(key)
		if taskKey == "" {
			continue
		}
		b.runningTask[taskKey] = struct{}{}
	}
	b.mtx.Unlock()

	// Watch
	watchCtx, watchCancel := context.WithTimeout(ctx, time.Minute*10)
	defer watchCancel()

	watchOpts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithKeysOnly(),
		clientv3.WithRev(resp.Header.Revision),
	}
	wc := b.client.Watch(watchCtx, runningTaskPrefix, watchOpts...)
	for wresp := range wc {
		if wresp.Err() != nil {
			return wresp.Err()
		}

		b.mtx.Lock()
		for _, ev := range wresp.Events {
			key := string(ev.Kv.Key)
			taskKey := findTaskKey(key)
			if taskKey == "" {
				continue
			}
			if ev.Type == clientv3.EventTypeDelete {
				delete(b.runningTask, taskKey)
			}

			if ev.Type == clientv3.EventTypePut {
				b.runningTask[taskKey] = struct{}{}
			}
		}
		b.mtx.Unlock()
	}

	return nil
}

func (b *etcdBroker) listWatchPendingTask(ctx context.Context, queue string) error {
	keyPrefix := fmt.Sprintf("%s/%s", pendingTaskPrefix, queue)

	// List
	listCtx, listCancel := context.WithTimeout(ctx, time.Second*10)
	defer listCancel()
	resp, err := b.client.Get(listCtx, keyPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return err
	}

	b.mtx.Lock()
	b.pendingTask = make(map[string]struct{})
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		taskKey := findTaskKey(key)
		if taskKey == "" {
			continue
		}
		b.pendingTask[taskKey] = struct{}{}
	}
	b.mtx.Unlock()

	// Watch
	watchCtx, watchCancel := context.WithTimeout(ctx, time.Minute*10)
	defer watchCancel()

	watchOpts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithKeysOnly(),
		clientv3.WithRev(resp.Header.Revision),
	}
	wc := b.client.Watch(watchCtx, keyPrefix, watchOpts...)
	for wresp := range wc {
		if wresp.Err() != nil {
			return wresp.Err()
		}

		b.mtx.Lock()
		for _, ev := range wresp.Events {
			key := string(ev.Kv.Key)
			taskKey := findTaskKey(key)
			if taskKey == "" {
				continue
			}
			if ev.Type == clientv3.EventTypeDelete {
				delete(b.pendingTask, taskKey)
			}

			if ev.Type == clientv3.EventTypePut {
				b.pendingTask[taskKey] = struct{}{}
			}
		}
		b.mtx.Unlock()
	}

	return nil
}

func getQueue(config *config.Config, taskProcessor iface.TaskProcessor) string {
	customQueue := taskProcessor.CustomQueue()
	if customQueue == "" {
		return config.DefaultQueue
	}
	return customQueue
}

// findTaskKey return {queue}/{taskID}
func findTaskKey(key string) string {
	switch {
	case strings.HasPrefix(key, pendingTaskPrefix+"/"):
		return key[len(pendingTaskPrefix)+1:]

	case strings.HasPrefix(key, runningTaskPrefix+"/"):
		return key[len(runningTaskPrefix)+1:]

	case strings.HasPrefix(key, delayedTaskPrefix):
		// {delayedTaskPrefix}/eta-{ms}/{queue}/{taskID}
		parts := strings.Split(key, "/")
		if len(parts) != 8 {
			log.WARNING.Printf("invalid delay task %s, just ignore", key)
			return ""
		}
		return fmt.Sprintf("%s/%s", parts[6], parts[7])

	default:
		log.WARNING.Printf("invalid task %s, just ignore", key)
		return ""
	}
}
