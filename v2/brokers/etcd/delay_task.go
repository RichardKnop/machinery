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

package etcd

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/RichardKnop/machinery/v2/log"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"golang.org/x/sync/errgroup"
)

const (
	// delayTaskMaxETA is the maximum eta duration for list&watch delayed task
	delayTaskMaxETA = time.Minute * 10
)

type delayTask struct {
	key       string    // {delayedTaskPrefix}/eta-{ms}/{queue}/{taskID}
	taskKey   string    // {queue}/{taskID}
	eta       time.Time // eta-{ms}
	kv        *mvccpb.KeyValue
	bindValue *mvccpb.KeyValue
}

func makeDelayTask(kv *mvccpb.KeyValue) (*delayTask, error) {
	key := string(kv.Key)
	// {delayedTaskPrefix}/eta-{ms}/{queue}/{taskID}
	parts := strings.Split(key, "/")
	if len(parts) != 8 {
		return nil, fmt.Errorf("invalid key")
	}

	taskKey := fmt.Sprintf("%s/%s", parts[6], parts[7])

	// eta-{ms} -> {ms}
	etaStr := strings.TrimPrefix(parts[5], "eta-")

	etaMilli, err := strconv.Atoi(etaStr)
	if err != nil {
		return nil, fmt.Errorf("invalid eta")
	}

	task := &delayTask{
		key:     key,
		taskKey: taskKey,
		eta:     time.UnixMilli(int64(etaMilli)),
		kv:      kv,
	}
	return task, nil
}

func (b *etcdBroker) listWatchDelayedTask(ctx context.Context) error {
	keyPrefix := fmt.Sprintf("%s/eta-", delayedTaskPrefix)
	endTime := time.Now().Add(delayTaskMaxETA)

	// List
	listCtx, listCancel := context.WithTimeout(ctx, time.Second*10)
	defer listCancel()

	end := strconv.FormatInt(endTime.UnixMilli(), 10)
	rangeOpts := []clientv3.OpOption{clientv3.WithRange(keyPrefix + end)}
	resp, err := b.client.Get(listCtx, keyPrefix+"0", rangeOpts...)
	if err != nil {
		return err
	}

	b.delayedMtx.Lock()
	// 清空数据
	b.delayedTask = make(map[string]*delayTask)

	for _, ev := range resp.Kvs {
		task, err := makeDelayTask(ev)
		if err != nil {
			log.ERROR.Printf("make delay task %s failed, err: %s", ev.Key, err)
			continue
		}
		b.delayedTask[task.key] = task
	}
	b.delayedMtx.Unlock()

	// Watch
	watchCtx, watchCancel := context.WithTimeout(ctx, delayTaskMaxETA)
	defer watchCancel()

	eg, egCtx := errgroup.WithContext(watchCtx)
	eg.Go(func() error {
		watchOpts := []clientv3.OpOption{
			clientv3.WithPrefix(),
			clientv3.WithKeysOnly(),
			clientv3.WithRev(resp.Header.Revision),
		}
		wc := b.client.Watch(egCtx, keyPrefix, watchOpts...)
		for wresp := range wc {
			if wresp.Err() != nil {
				return wresp.Err()
			}

			b.delayedMtx.Lock()
			for _, ev := range wresp.Events {
				task, err := makeDelayTask(ev.Kv)
				if err != nil {
					log.ERROR.Printf("make delay task %s failed, err: %s", ev.Kv.Key, err)
					continue
				}
				if ev.Type == clientv3.EventTypeDelete {
					delete(b.delayedTask, task.key)
				}

				if ev.Type == clientv3.EventTypePut {
					b.delayedTask[task.key] = task
				}
			}
			b.delayedMtx.Unlock()
		}
		return nil
	})

	eg.Go(func() error {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()

		for {
			select {
			case <-egCtx.Done():
				return nil
			case <-tick.C:
				b.handleDelayTask(egCtx)
			}
		}
	})

	return eg.Wait()
}

func (b *etcdBroker) handleDelayedTask(ctx context.Context) error {
	s, err := concurrency.NewSession(b.client)
	if err != nil {
		return err
	}
	defer s.Close() // nolint

	m := concurrency.NewMutex(s, delayedTaskLockKey)

	// 最长等待watch时间获取锁
	lockCtx, lockCancel := context.WithTimeout(ctx, delayTaskMaxETA)
	defer lockCancel()

	log.INFO.Printf("try acquire delayed task lock")
	if err = m.Lock(lockCtx); err != nil {
		log.INFO.Printf("try acquire delayed task lock failed, err: %s", err)
		return err
	}
	log.INFO.Printf("acquire delayed task lock done")

	defer func() {
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), time.Second*5)
		defer unlockCancel()

		if err = m.Unlock(unlockCtx); err != nil {
			log.ERROR.Printf("unlock delayed task failed, err: %s", err)
		}
	}()

	log.INFO.Printf("start handle delayed task")
	err = b.listWatchDelayedTask(ctx)
	log.INFO.Printf("handle delayed task done, err: %v", err)

	return err
}

func (b *etcdBroker) handleDelayTask(ctx context.Context) {
	now := time.Now()
	taskList := []*delayTask{}

	b.delayedMtx.Lock()
	for _, task := range b.delayedTask {
		if task.eta.Before(now) {
			taskList = append(taskList, task)
		}
	}
	b.delayedMtx.Unlock()

	// 最老的任务最快处理
	sort.Slice(taskList, func(i, j int) bool {
		return taskList[i].eta.Before(taskList[j].eta)
	})

	// 异步任务随时可能插入, 最多处理1分钟后重新获取任务列表(aka 异步任务到期后, 最多延迟1分钟放到pending队列)
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	for _, task := range taskList {
		// 超时控制
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := b.ensureDelayTaskBody(ctx, task); err != nil {
			log.ERROR.Printf("ensure delay task body %s failed, diff=%s, err=%s", task.key, time.Since(task.eta), err)
			continue
		}

		if err := b.moveToPendingTask(ctx, task); err != nil {
			log.ERROR.Printf("move delay task %s failed, diff=%s, err=%s", task.key, time.Since(task.eta), err)
			continue
		}

		b.delayedMtx.Lock()
		delete(b.delayedTask, task.key)
		b.delayedMtx.Unlock()
	}
}

func (b *etcdBroker) ensureDelayTaskBody(ctx context.Context, task *delayTask) error {
	if task.bindValue != nil {
		return nil
	}

	delayKeyNotChange := clientv3.Compare(clientv3.ModRevision(task.key), "=", task.kv.ModRevision)
	getReq := clientv3.OpGet(task.key)
	resp, err := b.client.Txn(ctx).If(delayKeyNotChange).Then(getReq).Commit()
	if err != nil {
		return err
	}

	if len(resp.Responses) != 1 {
		return fmt.Errorf("tnx resp invalid, count=%d", len(resp.Responses))
	}

	getResp := resp.Responses[0].GetResponseRange()
	if len(getResp.Kvs) == 0 || len(getResp.Kvs[0].Value) == 0 {
		return fmt.Errorf("have no body")
	}

	task.bindValue = getResp.Kvs[0]
	return nil
}

func (b *etcdBroker) moveToPendingTask(ctx context.Context, task *delayTask) error {
	pendingKey := fmt.Sprintf("%s/%s", pendingTaskPrefix, task.taskKey)
	delayKeyNotChange := clientv3.Compare(clientv3.ModRevision(task.key), "=", task.kv.ModRevision)
	pendingKeyNotExist := clientv3.Compare(clientv3.CreateRevision(pendingKey), "=", 0)

	deleteReq := clientv3.OpDelete(task.key)
	putReq := clientv3.OpPut(pendingKey, string(task.kv.Value))
	c, err := b.client.Txn(ctx).If(delayKeyNotChange, pendingKeyNotExist).Then(deleteReq, putReq).Commit()
	if err != nil {
		return err
	}
	if !c.Succeeded {
		return fmt.Errorf("txn not success, maybe key conflict, will retry later")
	}

	log.DEBUG.Printf("move delay task %s to pending queue done, diff=%s", task.key, time.Since(task.eta))
	return nil
}
