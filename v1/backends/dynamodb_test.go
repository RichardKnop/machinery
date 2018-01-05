package backends_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/backends"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
)

func TestNewDynamoDBBackend(t *testing.T) {
	backend := backends.NewDynamoDBBackend(backends.TestCnf)
	assert.IsType(t, &backends.DynamoDBBackend{}, backend)
}

func TestInitGroup(t *testing.T) {
	groupUUID := "testGroupUUID"
	taskUUIDs = []string{"testTaskUUID1", "testTaskUUID2", "testTaskUUID3"}
	log.INFO.Println(backends.TestDynamoDBBackend.GetConfig())
	err := backends.TestDynamoDBBackend.InitGroup(groupUUID, taskUUIDs)
	assert.Nil(t, err)

	err = backends.TestErrDynamoDBBackend.InitGroup(groupUUID, taskUUIDs)
	assert.NotNil(t, err)
}

func TestDynamoDBGroupCompleted(t *testing.T) {

	task1 := map[string]*dynamodb.AttributeValue{
		"Error": {
			NULL: aws.Bool(true),
		},
		"State": {
			S: aws.String(tasks.StatePending),
		},
		"TaskUUID": {
			S: aws.String("testTaskUUID1"),
		},
		"Results:": {
			NULL: aws.Bool(true),
		},
	}
	task2 := map[string]*dynamodb.AttributeValue{
		"Error": {
			NULL: aws.Bool(true),
		},
		"State": {
			S: aws.String(tasks.StateStarted),
		},
		"TaskUUID": {
			S: aws.String("testTaskUUID2"),
		},
		"Results:": {
			NULL: aws.Bool(true),
		},
	}
	task3 := map[string]*dynamodb.AttributeValue{
		"Error": {
			NULL: aws.Bool(true),
		},
		"State": {
			S: aws.String(tasks.StateSuccess),
		},
		"TaskUUID": {
			S: aws.String("testTaskUUID3"),
		},
		"Results:": {
			NULL: aws.Bool(true),
		},
	}

	backends.TestTask1, backends.TestTask2, backends.TestTask3 = task1, task2, task3

	groupUUID := "testGroupUUID"
	taskUUIDs = []string{"testTaskUUID1", "testTaskUUID2", "testTaskUUID3"}
	_, err := backends.TestErrDynamoDBBackend.GroupCompleted(groupUUID, 3)
	assert.NotNil(t, err)
	completed, err := backends.TestDynamoDBBackend.GroupCompleted(groupUUID, 3)
	assert.False(t, completed)
	assert.Nil(t, err)

	task1["State"] = &dynamodb.AttributeValue{
		S: aws.String(tasks.StateFailure),
	}
	task2["State"] = &dynamodb.AttributeValue{
		S: aws.String(tasks.StateSuccess),
	}
	completed, err = backends.TestDynamoDBBackend.GroupCompleted(groupUUID, 3)
	assert.True(t, completed)
	assert.Nil(t, err)
}

func TestDynamoDBPrivateFuncGetGroupMeta(t *testing.T) {
	groupUUID := "testGroupUUID"
	meta, err := backends.TestDynamoDBBackend.GetGroupMetaForTest(groupUUID)
	item := tasks.GroupMeta{
		GroupUUID:      "testGroupUUID",
		Lock:           false,
		ChordTriggered: false,
		TaskUUIDs: []string{
			"testTaskUUID1",
			"testTaskUUID2",
			"testTaskUUID3",
		},
	}
	assert.Nil(t, err)
	assert.EqualValues(t, item, *meta)
	_, err = backends.TestErrDynamoDBBackend.GetGroupMetaForTest(groupUUID)
	assert.NotNil(t, err)
}

func TestDynamoDBPrivateFuncUnmarshalTaskStateGetItemResult(t *testing.T) {
	result := dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"Error": {
				NULL: aws.Bool(true),
			},
			"State": {
				S: aws.String(tasks.StatePending),
			},
			"TaskUUID": {
				S: aws.String("testTaskUUID1"),
			},
			"Results:": {
				NULL: aws.Bool(true),
			},
		},
	}

	invalidResult := dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"Error": {
				BOOL: aws.Bool(true),
			},
			"State": {
				S: aws.String(tasks.StatePending),
			},
			"TaskUUID": {
				S: aws.String("testTaskUUID1"),
			},
			"Results:": {
				BOOL: aws.Bool(true),
			},
		},
	}

	item := tasks.TaskState{
		TaskUUID: "testTaskUUID1",
		Results:  nil,
		State:    tasks.StatePending,
		Error:    "",
	}
	state, err := backends.TestErrDynamoDBBackend.UnmarshalTaskStateGetItemResultForTest(&result)
	assert.Nil(t, err)
	assert.EqualValues(t, item, *state)

	_, err = backends.TestDynamoDBBackend.UnmarshalTaskStateGetItemResultForTest(nil)
	assert.NotNil(t, err)

	_, err = backends.TestDynamoDBBackend.UnmarshalTaskStateGetItemResultForTest(&invalidResult)
	assert.NotNil(t, err)

}

func TestDynamoDBPrivateFuncUnmarshalGroupMetaGetItemResult(t *testing.T) {
	result := dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"TaskUUIDs": {
				L: []*dynamodb.AttributeValue{
					{
						S: aws.String("testTaskUUID1"),
					},
					{
						S: aws.String("testTaskUUID2"),
					},
					{
						S: aws.String("testTaskUUID3"),
					},
				},
			},
			"ChordTriggered": {
				BOOL: aws.Bool(false),
			},
			"GroupUUID": {
				S: aws.String("testGroupUUID"),
			},
			"Lock": {
				BOOL: aws.Bool(false),
			},
		},
	}

	invalidResult := dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"TaskUUIDs": {
				L: []*dynamodb.AttributeValue{
					{
						S: aws.String("testTaskUUID1"),
					},
					{
						S: aws.String("testTaskUUID2"),
					},
					{
						S: aws.String("testTaskUUID3"),
					},
				},
			},
			"ChordTriggered": {
				S: aws.String("false"), // this attribute is invalid
			},
			"GroupUUID": {
				S: aws.String("testGroupUUID"),
			},
			"Lock": {
				BOOL: aws.Bool(false),
			},
		},
	}

	item := tasks.GroupMeta{
		GroupUUID:      "testGroupUUID",
		Lock:           false,
		ChordTriggered: false,
		TaskUUIDs: []string{
			"testTaskUUID1",
			"testTaskUUID2",
			"testTaskUUID3",
		},
	}
	meta, err := backends.TestErrDynamoDBBackend.UnmarshalGroupMetaGetItemResultForTest(&result)
	assert.Nil(t, err)
	assert.EqualValues(t, item, *meta)
	_, err = backends.TestErrDynamoDBBackend.UnmarshalGroupMetaGetItemResultForTest(nil)
	assert.NotNil(t, err)

	_, err = backends.TestErrDynamoDBBackend.UnmarshalGroupMetaGetItemResultForTest(&invalidResult)
	assert.NotNil(t, err)

}

func TestDynamoDBPrivateFuncSetTaskState(t *testing.T) {
	signature := &tasks.Signature{
		Name: "Test",
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: 1,
			},
		},
	}
	state := tasks.NewPendingTaskState(signature)
	err := backends.TestErrDynamoDBBackend.SetTaskStateForTest(state)
	assert.NotNil(t, err)
	err = backends.TestDynamoDBBackend.SetTaskStateForTest(state)
	assert.Nil(t, err)
}

func TestDynamoDBPrivateFuncGetStates(t *testing.T) {
	task1 := map[string]*dynamodb.AttributeValue{
		"Error": {
			NULL: aws.Bool(true),
		},
		"State": {
			S: aws.String(tasks.StatePending),
		},
		"TaskUUID": {
			S: aws.String("testTaskUUID1"),
		},
		"Results:": {
			NULL: aws.Bool(true),
		},
	}
	task2 := map[string]*dynamodb.AttributeValue{
		"Error": {
			NULL: aws.Bool(true),
		},
		"State": {
			S: aws.String(tasks.StateStarted),
		},
		"TaskUUID": {
			S: aws.String("testTaskUUID2"),
		},
		"Results:": {
			NULL: aws.Bool(true),
		},
	}
	task3 := map[string]*dynamodb.AttributeValue{
		"Error": {
			NULL: aws.Bool(true),
		},
		"State": {
			S: aws.String(tasks.StateSuccess),
		},
		"TaskUUID": {
			S: aws.String("testTaskUUID3"),
		},
		"Results:": {
			NULL: aws.Bool(true),
		},
	}
	backends.TestTask1, backends.TestTask2, backends.TestTask3 = task1, task2, task3

	taskUUIDs := []string{
		"testTaskUUID1",
		"testTaskUUID2",
		"testTaskUUID3",
	}
	expectedStates := map[string]*tasks.TaskState{
		"testTaskUUID1": {
			TaskUUID: "testTaskUUID1",
			Results:  nil,
			State:    tasks.StatePending,
			Error:    "",
		},
		"testTaskUUID2": {
			TaskUUID: "testTaskUUID2",
			Results:  nil,
			State:    tasks.StateStarted,
			Error:    "",
		},
		"testTaskUUID3": {
			TaskUUID: "testTaskUUID3",
			Results:  nil,
			State:    tasks.StateSuccess,
			Error:    "",
		},
	}
	states, err := backends.TestDynamoDBBackend.GetStatesForTest(taskUUIDs...)
	assert.Nil(t, err)

	for _, s := range states {
		assert.EqualValues(t, *s, *expectedStates[s.TaskUUID])
	}
}

func TestDynamoDBGroupTaskStates(t *testing.T) {
	groupUUID := "testGroupUUID"
	count := 3
	task1 := map[string]*dynamodb.AttributeValue{
		"Error": {
			NULL: aws.Bool(true),
		},
		"State": {
			S: aws.String(tasks.StatePending),
		},
		"TaskUUID": {
			S: aws.String("testTaskUUID1"),
		},
		"Results:": {
			NULL: aws.Bool(true),
		},
	}
	task2 := map[string]*dynamodb.AttributeValue{
		"Error": {
			NULL: aws.Bool(true),
		},
		"State": {
			S: aws.String(tasks.StateStarted),
		},
		"TaskUUID": {
			S: aws.String("testTaskUUID2"),
		},
		"Results:": {
			NULL: aws.Bool(true),
		},
	}
	task3 := map[string]*dynamodb.AttributeValue{
		"Error": {
			NULL: aws.Bool(true),
		},
		"State": {
			S: aws.String(tasks.StateSuccess),
		},
		"TaskUUID": {
			S: aws.String("testTaskUUID3"),
		},
		"Results:": {
			NULL: aws.Bool(true),
		},
	}
	backends.TestTask1, backends.TestTask2, backends.TestTask3 = task1, task2, task3
	expectedStates := map[string]*tasks.TaskState{
		"testTaskUUID1": {
			TaskUUID: "testTaskUUID1",
			Results:  nil,
			State:    tasks.StatePending,
			Error:    "",
		},
		"testTaskUUID2": {
			TaskUUID: "testTaskUUID2",
			Results:  nil,
			State:    tasks.StateStarted,
			Error:    "",
		},
		"testTaskUUID3": {
			TaskUUID: "testTaskUUID3",
			Results:  nil,
			State:    tasks.StateSuccess,
			Error:    "",
		},
	}

	states, err := backends.TestDynamoDBBackend.GroupTaskStates(groupUUID, count)
	assert.Nil(t, err)
	for _, s := range states {
		assert.EqualValues(t, *s, *expectedStates[s.TaskUUID])
	}
}

func TestDynamoDBTriggerChord(t *testing.T) {
	groupUUID := "testGroupUUID"
	triggered, err := backends.TestDynamoDBBackend.TriggerChord(groupUUID)
	assert.Nil(t, err)
	assert.True(t, triggered)
}

func TestDynamoDBGetState(t *testing.T) {
	taskUUID := "testTaskUUID1"
	expectedState := &tasks.TaskState{
		TaskUUID: "testTaskUUID1",
		Results:  nil,
		State:    tasks.StatePending,
		Error:    "",
	}
	state, err := backends.TestDynamoDBBackend.GetState(taskUUID)
	assert.Nil(t, err)
	assert.EqualValues(t, expectedState, state)
}

func TestDynamoDBPurgeState(t *testing.T) {
	taskUUID := "testTaskUUID1"
	err := backends.TestDynamoDBBackend.PurgeState(taskUUID)
	assert.Nil(t, err)

	err = backends.TestErrDynamoDBBackend.PurgeState(taskUUID)
	assert.NotNil(t, err)
}

func TestDynamoDBPurgeGroupMeta(t *testing.T) {
	groupUUID := "GroupUUID"
	err := backends.TestDynamoDBBackend.PurgeGroupMeta(groupUUID)
	assert.Nil(t, err)

	err = backends.TestErrDynamoDBBackend.PurgeGroupMeta(groupUUID)
	assert.NotNil(t, err)
}

func TestDynamoDBPrivateFuncLockGroupMeta(t *testing.T) {
	groupUUID := "GroupUUID"
	err := backends.TestDynamoDBBackend.LockGroupMetaForTest(groupUUID)
	assert.Nil(t, err)
	err = backends.TestErrDynamoDBBackend.LockGroupMetaForTest(groupUUID)
	assert.NotNil(t, err)
}

func TestDynamoDBPrivateFuncUnLockGroupMeta(t *testing.T) {
	groupUUID := "GroupUUID"
	err := backends.TestDynamoDBBackend.UnlockGroupMetaForTest(groupUUID)
	assert.Nil(t, err)
	err = backends.TestErrDynamoDBBackend.UnlockGroupMetaForTest(groupUUID)
	assert.NotNil(t, err)
}

func TestDynamoDBPrivateFuncChordTriggered(t *testing.T) {
	groupUUID := "GroupUUID"
	err := backends.TestDynamoDBBackend.ChordTriggeredForTest(groupUUID)
	assert.Nil(t, err)
	err = backends.TestErrDynamoDBBackend.ChordTriggeredForTest(groupUUID)
	assert.NotNil(t, err)
}

func TestDynamoDBPrivateFuncUpdateGroupMetaLock(t *testing.T) {
	groupUUID := "GroupUUID"
	err := backends.TestDynamoDBBackend.UpdateGroupMetaLockForTest(groupUUID, true)
	assert.Nil(t, err)
	err = backends.TestErrDynamoDBBackend.UpdateGroupMetaLockForTest(groupUUID, true)
	assert.NotNil(t, err)
}

func TestDynamoDBPrivateFuncUpdateToFailureStateWithError(t *testing.T) {
	signature := &tasks.Signature{
		Name: "Test",
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: 1,
			},
		},
	}

	state := tasks.NewFailureTaskState(signature, "This is an error")
	err := backends.TestDynamoDBBackend.UpdateToFailureStateWithErrorForTest(state)
	assert.Nil(t, err)
}

func TestDynamoDBPrivateFuncTableExistsForTest(t *testing.T) {
	tables := []*string{aws.String("foo")}
	assert.False(t, backends.TestDynamoDBBackend.TableExistsForTest("bar", tables))
	assert.True(t, backends.TestDynamoDBBackend.TableExistsForTest("foo", tables))
}

func TestDynamoDBPrivateFuncCheckRequiredTablesIfExistForTest(t *testing.T) {
	err := backends.TestDynamoDBBackend.CheckRequiredTablesIfExistForTest()
	assert.Nil(t, err)
	taskTable := backends.TestDynamoDBBackend.GetConfig().DynamoDB.TaskStatesTable
	groupTable := backends.TestDynamoDBBackend.GetConfig().DynamoDB.GroupMetasTable
	err = backends.TestErrDynamoDBBackend.CheckRequiredTablesIfExistForTest()
	assert.NotNil(t, err)
	backends.TestDynamoDBBackend.GetConfig().DynamoDB.TaskStatesTable = "foo"
	err = backends.TestDynamoDBBackend.CheckRequiredTablesIfExistForTest()
	assert.NotNil(t, err)
	backends.TestDynamoDBBackend.GetConfig().DynamoDB.TaskStatesTable = taskTable
	backends.TestDynamoDBBackend.GetConfig().DynamoDB.GroupMetasTable = "foo"
	err = backends.TestDynamoDBBackend.CheckRequiredTablesIfExistForTest()
	assert.NotNil(t, err)
	backends.TestDynamoDBBackend.GetConfig().DynamoDB.GroupMetasTable = groupTable

}
