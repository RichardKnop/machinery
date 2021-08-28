package sql

import (
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func newBackend(t *testing.T) iface.Backend {
	url := os.Getenv("SQL_URL")
	if url == "" {
		t.Skip("SQL_URL is not defined")
	}
	db, err := gorm.Open(mysql.Open(url), &gorm.Config{
		PrepareStmt: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanUp(db)
	if err := db.AutoMigrate(&TaskState{}, &GroupMetadata{}); err != nil {
		t.Fatal(err)
	}

	cnf := &config.Config{
		SQL: &config.SqlDBConfig{Client: db},
	}
	return New(cnf)
}

func cleanUp(db *gorm.DB) {
	db.Where("1 = 1").Delete(&TaskState{})
	db.Where("1 = 1").Delete(&GroupMetadata{})
}

func TestGroupCompleted(t *testing.T) {
	backend := newBackend(t)

	groupUUID := "testGroupUUID"
	task1 := &tasks.Signature{
		UUID:      "testTaskUUID1",
		GroupUUID: groupUUID,
	}
	task2 := &tasks.Signature{
		UUID:      "testTaskUUID2",
		GroupUUID: groupUUID,
	}

	assert.Nil(t, backend.SetStatePending(task1))
	assert.Nil(t, backend.SetStatePending(task2))
	assert.Nil(t, backend.InitGroup(groupUUID, []string{task1.UUID, task2.UUID}))

	tests := []struct {
		taskStateS []string
		want       bool
	}{
		{[]string{tasks.StatePending, tasks.StatePending}, false},
		{[]string{tasks.StateReceived, tasks.StatePending}, false},
		{[]string{tasks.StateReceived, tasks.StateReceived}, false},
		{[]string{tasks.StateReceived, tasks.StateStarted}, false},
		{[]string{tasks.StateReceived, tasks.StateRetry}, false},
		{[]string{tasks.StateStarted, tasks.StateRetry}, false},
		{[]string{tasks.StateSuccess, tasks.StateRetry}, false},
		{[]string{tasks.StateSuccess, tasks.StateFailure}, true},
	}
	for _, tt := range tests {
		t.Run(strings.Join(tt.taskStateS, ", "), func(t *testing.T) {
			for i, task := range []*tasks.Signature{task1, task2} {
				switch tt.taskStateS[i] {
				case tasks.StatePending:
					break
				case tasks.StateReceived:
					assert.Nil(t, backend.SetStateReceived(task))
				case tasks.StateStarted:
					assert.Nil(t, backend.SetStateStarted(task))
				case tasks.StateRetry:
					assert.Nil(t, backend.SetStateRetry(task))
				case tasks.StateSuccess:
					assert.Nil(t, backend.SetStateSuccess(task, []*tasks.TaskResult{}))
				case tasks.StateFailure:
					assert.Nil(t, backend.SetStateFailure(task, "error"))
				}
			}

			ok, err := backend.GroupCompleted(groupUUID, len(tt.taskStateS))
			assert.Nil(t, err)
			assert.Equal(t, tt.want, ok)
		})
	}
}

func TestGroupTaskStates(t *testing.T) {
	backend := newBackend(t)

	groupUUID := "testGroupUUID"
	task1 := &tasks.Signature{
		UUID:      "testTaskUUID1",
		GroupUUID: groupUUID,
	}
	task2 := &tasks.Signature{
		UUID:      "testTaskUUID2",
		GroupUUID: groupUUID,
	}

	assert.Nil(t, backend.SetStatePending(task1))
	assert.Nil(t, backend.SetStatePending(task2))
	assert.Nil(t, backend.InitGroup(groupUUID, []string{task1.UUID, task2.UUID}))

	taskStates, err := backend.GroupTaskStates(groupUUID, 0)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(taskStates))
}

func TestTriggerChord(t *testing.T) {
	backend := newBackend(t)

	groupUUID := "testGroupUUID"
	task1 := &tasks.Signature{
		UUID:      "testTaskUUID1",
		GroupUUID: groupUUID,
	}
	task2 := &tasks.Signature{
		UUID:      "testTaskUUID2",
		GroupUUID: groupUUID,
	}

	assert.Nil(t, backend.SetStatePending(task1))
	assert.Nil(t, backend.SetStatePending(task2))
	assert.Nil(t, backend.InitGroup(groupUUID, []string{task1.UUID, task2.UUID}))

	okCount := 0
	wg := sync.WaitGroup{}
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			ok, err := backend.TriggerChord(groupUUID)
			assert.Nil(t, err)
			if ok {
				okCount += 1
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, okCount, 1)
}

func TestGetState(t *testing.T) {
	backend := newBackend(t)

	groupUUID := "testGroupUUID"
	task := &tasks.Signature{
		Name:      "testTask01",
		UUID:      "testTaskUUID1",
		GroupUUID: groupUUID,
	}
	assert.Nil(t, backend.SetStatePending(task))
	errorStr := uuid.New().String()
	result := []*tasks.TaskResult{
		{"float64", 3.3},
		{"int64", float64(33)},
		{"bool", false},
		{"string", "abc"},
		{"[]bool", nil},
		{"[]bool", []interface{}{false, false, true}},
		{"[]string", nil},
		{"[]string", []interface{}{"a", "b", "c"}},
	}

	assert.Nil(t, backend.SetStateSuccess(task, result))
	taskState, err := backend.GetState(task.UUID)
	assert.Nil(t, err)
	assert.Equal(t, tasks.StateSuccess, taskState.State)
	assert.Equal(t, result, taskState.Results)

	assert.Nil(t, backend.SetStateFailure(task, errorStr))
	taskState, err = backend.GetState(task.UUID)
	assert.Nil(t, err)
	assert.Equal(t, tasks.StateFailure, taskState.State)
	assert.Equal(t, errorStr, taskState.Error)
}

func TestPurge(t *testing.T) {
	backend := newBackend(t)

	groupUUID := "testGroupUUID"
	task1 := &tasks.Signature{
		UUID:      "testTaskUUID1",
		GroupUUID: groupUUID,
	}
	task2 := &tasks.Signature{
		UUID:      "testTaskUUID2",
		GroupUUID: groupUUID,
	}

	assert.Nil(t, backend.SetStatePending(task1))
	assert.Nil(t, backend.SetStatePending(task2))
	assert.Nil(t, backend.InitGroup(groupUUID, []string{task1.UUID, task2.UUID}))

	for _, taskUUID := range []string{task1.UUID, task2.UUID} {
		taskState, err := backend.GetState(taskUUID)
		assert.Nil(t, err, taskUUID)
		assert.NotNil(t, taskState, taskUUID)
	}

	assert.Nil(t, backend.PurgeState(task1.UUID))
	taskState1, err := backend.GetState(task1.UUID)
	assert.Equal(t, gorm.ErrRecordNotFound, err)
	assert.Nil(t, taskState1)

	assert.Nil(t, backend.PurgeGroupMeta(groupUUID))
	groupMeta, err := backend.GroupTaskStates(groupUUID, 0)
	assert.Equal(t, gorm.ErrRecordNotFound, err)
	assert.Nil(t, groupMeta)

	taskState2, err := backend.GetState(task2.UUID)
	assert.Nil(t, err)
	assert.NotNil(t, taskState2)
}
