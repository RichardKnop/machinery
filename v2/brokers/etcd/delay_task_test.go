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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestMakeDelayTask(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		taskKey string
		eta     time.Time
	}{
		{
			name:    "delayed_task1",
			input:   "/machinery/v2/broker/delayed_tasks/eta-1/machinery_tasks/d30986b4-6634-4013-bf56-88c0463450c2-test-0",
			taskKey: "machinery_tasks/d30986b4-6634-4013-bf56-88c0463450c2-test-0",
			eta:     time.UnixMilli(1),
		},
		{
			name:    "delayed_task2",
			input:   "/machinery/v2/broker/delayed_tasks/eta-0/machinery_tasks/d30986b4-6634-4013-bf56-88c0463450c2-test-0",
			taskKey: "machinery_tasks/d30986b4-6634-4013-bf56-88c0463450c2-test-0",
			eta:     time.UnixMilli(0),
		},
		{
			name:    "delayed_task3",
			input:   "/machinery/v2/broker/delayed_tasks/eta-1732356480593/machinery_tasks/d30986b4-6634-4013-bf56-88c0463450c2-test-0",
			taskKey: "machinery_tasks/d30986b4-6634-4013-bf56-88c0463450c2-test-0",
			eta:     time.UnixMilli(1732356480593),
		},
		{
			name:    "delayed_task4",
			input:   "/machinery/v2/broker/delayed_tasks/eta-1732356480583/machinery_tasks/d30986b4-6634-4013-bf56-88c0463450c2-test-0",
			taskKey: "machinery_tasks/d30986b4-6634-4013-bf56-88c0463450c2-test-0",
			eta:     time.UnixMilli(1732356480583),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kv := &mvccpb.KeyValue{Key: []byte(tt.input)}
			task, err := makeDelayTask(kv)
			require.NoError(t, err)
			assert.Equal(t, tt.input, task.key)
			assert.Equal(t, tt.taskKey, task.taskKey)
			assert.Equal(t, tt.eta, task.eta)
		})
	}
}
