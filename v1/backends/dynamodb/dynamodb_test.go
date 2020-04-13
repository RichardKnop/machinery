package dynamodb_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/backends/dynamodb"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"

	awsdynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestNew(t *testing.T) {
	// should call t.Skip if not connected to internet
	backend := dynamodb.New(dynamodb.TestCnf)
	assert.IsType(t, new(dynamodb.Backend), backend)
}

func TestInitGroup(t *testing.T) {
	groupUUID := "testGroupUUID"
	taskUUIDs := []string{"testTaskUUID1", "testTaskUUID2", "testTaskUUID3"}
	log.INFO.Println(dynamodb.TestDynamoDBBackend.GetConfig())

	err := dynamodb.TestDynamoDBBackend.InitGroup(groupUUID, taskUUIDs)
	assert.Nil(t, err)

	err = dynamodb.TestErrDynamoDBBackend.InitGroup(groupUUID, taskUUIDs)
	assert.NotNil(t, err)

	// assert proper TTL value is set in InitGroup()
	dynamodb.TestDynamoDBBackend.GetConfig().ResultsExpireIn = 3 * 3600 // results should expire after 3 hours
	client := dynamodb.TestDynamoDBBackend.GetClient().(*dynamodb.TestDynamoDBClient)
	// Override DynamoDB PutItem() behavior
	var isPutItemCalled bool
	client.PutItemOverride = func(input *awsdynamodb.PutItemInput) (*awsdynamodb.PutItemOutput, error) {
		isPutItemCalled = true
		assert.NotNil(t, input)

		actualTTLStr := *input.Item["TTL"].N
		expectedTTLTime := time.Now().Add(3 * time.Hour)
		assertTTLValue(t, expectedTTLTime, actualTTLStr)

		return &awsdynamodb.PutItemOutput{}, nil
	}
	err = dynamodb.TestDynamoDBBackend.InitGroup(groupUUID, taskUUIDs)
	assert.Nil(t, err)
	assert.True(t, isPutItemCalled)
	client.ResetOverrides()
}

func assertTTLValue(t *testing.T, expectedTTLTime time.Time, actualEncodedTTLValue string) {
	actualTTLTimestamp, err := strconv.ParseInt(actualEncodedTTLValue, 10, 64)
	assert.Nil(t, err)
	actualTTLTime := time.Unix(actualTTLTimestamp, 0)
	assert.WithinDuration(t, expectedTTLTime, actualTTLTime, time.Second)
}

func TestGroupCompleted(t *testing.T) {
	task1 := map[string]*awsdynamodb.AttributeValue{
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
	task2 := map[string]*awsdynamodb.AttributeValue{
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
	task3 := map[string]*awsdynamodb.AttributeValue{
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

	dynamodb.TestTask1, dynamodb.TestTask2, dynamodb.TestTask3 = task1, task2, task3

	groupUUID := "testGroupUUID"
	_, err := dynamodb.TestErrDynamoDBBackend.GroupCompleted(groupUUID, 3)
	assert.NotNil(t, err)
	completed, err := dynamodb.TestDynamoDBBackend.GroupCompleted(groupUUID, 3)
	assert.False(t, completed)
	assert.Nil(t, err)

	task1["State"] = &awsdynamodb.AttributeValue{
		S: aws.String(tasks.StateFailure),
	}
	task2["State"] = &awsdynamodb.AttributeValue{
		S: aws.String(tasks.StateSuccess),
	}
	completed, err = dynamodb.TestDynamoDBBackend.GroupCompleted(groupUUID, 3)
	assert.True(t, completed)
	assert.Nil(t, err)
}

func TestPrivateFuncGetGroupMeta(t *testing.T) {
	groupUUID := "testGroupUUID"
	meta, err := dynamodb.TestDynamoDBBackend.GetGroupMetaForTest(groupUUID)
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
	_, err = dynamodb.TestErrDynamoDBBackend.GetGroupMetaForTest(groupUUID)
	assert.NotNil(t, err)
}

func TestPrivateFuncUnmarshalTaskStateGetItemResult(t *testing.T) {
	result := awsdynamodb.GetItemOutput{
		Item: map[string]*awsdynamodb.AttributeValue{
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

	invalidResult := awsdynamodb.GetItemOutput{
		Item: map[string]*awsdynamodb.AttributeValue{
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
	state, err := dynamodb.TestErrDynamoDBBackend.UnmarshalTaskStateGetItemResultForTest(&result)
	assert.Nil(t, err)
	assert.EqualValues(t, item, *state)

	_, err = dynamodb.TestDynamoDBBackend.UnmarshalTaskStateGetItemResultForTest(nil)
	assert.NotNil(t, err)

	_, err = dynamodb.TestDynamoDBBackend.UnmarshalTaskStateGetItemResultForTest(&invalidResult)
	assert.NotNil(t, err)

}

func TestPrivateFuncUnmarshalGroupMetaGetItemResult(t *testing.T) {
	result := awsdynamodb.GetItemOutput{
		Item: map[string]*awsdynamodb.AttributeValue{
			"TaskUUIDs": {
				L: []*awsdynamodb.AttributeValue{
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

	invalidResult := awsdynamodb.GetItemOutput{
		Item: map[string]*awsdynamodb.AttributeValue{
			"TaskUUIDs": {
				L: []*awsdynamodb.AttributeValue{
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
	meta, err := dynamodb.TestErrDynamoDBBackend.UnmarshalGroupMetaGetItemResultForTest(&result)
	assert.Nil(t, err)
	assert.EqualValues(t, item, *meta)
	_, err = dynamodb.TestErrDynamoDBBackend.UnmarshalGroupMetaGetItemResultForTest(nil)
	assert.NotNil(t, err)

	_, err = dynamodb.TestErrDynamoDBBackend.UnmarshalGroupMetaGetItemResultForTest(&invalidResult)
	assert.NotNil(t, err)

}

func TestPrivateFuncSetTaskState(t *testing.T) {
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
	err := dynamodb.TestErrDynamoDBBackend.SetTaskStateForTest(state)
	assert.NotNil(t, err)
	err = dynamodb.TestDynamoDBBackend.SetTaskStateForTest(state)
	assert.Nil(t, err)
}

// verifyUpdateInput is a helper function to verify valid dynamoDB update input.
func verifyUpdateInput(t *testing.T, input *awsdynamodb.UpdateItemInput, expectedTaskID string, expectedState string, expectedTTLTime time.Time) {
	assert.NotNil(t, input)

	// verify task ID
	assert.Equal(t, expectedTaskID, *input.Key["TaskUUID"].S)

	// verify task state
	assert.Equal(t, expectedState, *input.ExpressionAttributeValues[":s"].S)

	// Verify TTL
	if !expectedTTLTime.IsZero() {
		actualTTLStr := *input.ExpressionAttributeValues[":t"].N
		assertTTLValue(t, expectedTTLTime, actualTTLStr)
	}
}

func TestSetStateSuccess(t *testing.T) {
	signature := &tasks.Signature{UUID: "testTaskUUID"}

	// assert correct task ID, state and TTL value is set in SetStateSuccess()
	dynamodb.TestDynamoDBBackend.GetConfig().ResultsExpireIn = 3 * 3600 // results should expire after 3 hours
	client := dynamodb.TestDynamoDBBackend.GetClient().(*dynamodb.TestDynamoDBClient)
	// Override DynamoDB UpdateItem() behavior
	var isUpdateItemCalled bool
	client.UpdateItemOverride = func(input *awsdynamodb.UpdateItemInput) (*awsdynamodb.UpdateItemOutput, error) {
		isUpdateItemCalled = true
		verifyUpdateInput(t, input, signature.UUID, tasks.StateSuccess, time.Now().Add(3*time.Hour))
		return &awsdynamodb.UpdateItemOutput{}, nil
	}

	err := dynamodb.TestDynamoDBBackend.SetStateSuccess(signature, nil)
	assert.Nil(t, err)
	assert.True(t, isUpdateItemCalled)
	client.ResetOverrides()
}

func TestSetStateFailure(t *testing.T) {
	signature := &tasks.Signature{UUID: "testTaskUUID"}

	// assert correct task ID, state and TTL value is set in SetStateFailure()
	dynamodb.TestDynamoDBBackend.GetConfig().ResultsExpireIn = 2 * 3600 // results should expire after 2 hours
	client := dynamodb.TestDynamoDBBackend.GetClient().(*dynamodb.TestDynamoDBClient)
	// Override DynamoDB UpdateItem() behavior
	var isUpdateItemCalled bool
	client.UpdateItemOverride = func(input *awsdynamodb.UpdateItemInput) (*awsdynamodb.UpdateItemOutput, error) {
		isUpdateItemCalled = true
		verifyUpdateInput(t, input, signature.UUID, tasks.StateFailure, time.Now().Add(2*time.Hour))
		return &awsdynamodb.UpdateItemOutput{}, nil
	}

	err := dynamodb.TestDynamoDBBackend.SetStateFailure(signature, "Some error occurred")
	assert.Nil(t, err)
	assert.True(t, isUpdateItemCalled)
	client.ResetOverrides()
}

func TestSetStateReceived(t *testing.T) {
	signature := &tasks.Signature{UUID: "testTaskUUID"}

	// assert correct task ID, state and *no* TTL value is set in SetStateReceived()
	dynamodb.TestDynamoDBBackend.GetConfig().ResultsExpireIn = 2 * 3600 // results should expire after 2 hours (ignored for this state)
	client := dynamodb.TestDynamoDBBackend.GetClient().(*dynamodb.TestDynamoDBClient)
	var isUpdateItemCalled bool
	client.UpdateItemOverride = func(input *awsdynamodb.UpdateItemInput) (*awsdynamodb.UpdateItemOutput, error) {
		isUpdateItemCalled = true
		verifyUpdateInput(t, input, signature.UUID, tasks.StateReceived, time.Time{})
		return &awsdynamodb.UpdateItemOutput{}, nil
	}

	err := dynamodb.TestDynamoDBBackend.SetStateReceived(signature)
	assert.Nil(t, err)
	assert.True(t, isUpdateItemCalled)
	client.ResetOverrides()
}

func TestSetStateStarted(t *testing.T) {
	signature := &tasks.Signature{UUID: "testTaskUUID"}

	// assert correct task ID, state and *no* TTL value is set in SetStateStarted()
	dynamodb.TestDynamoDBBackend.GetConfig().ResultsExpireIn = 2 * 3600 // results should expire after 2 hours (ignored for this state)
	client := dynamodb.TestDynamoDBBackend.GetClient().(*dynamodb.TestDynamoDBClient)
	var isUpdateItemCalled bool
	client.UpdateItemOverride = func(input *awsdynamodb.UpdateItemInput) (*awsdynamodb.UpdateItemOutput, error) {
		isUpdateItemCalled = true
		verifyUpdateInput(t, input, signature.UUID, tasks.StateStarted, time.Time{})
		return &awsdynamodb.UpdateItemOutput{}, nil
	}

	err := dynamodb.TestDynamoDBBackend.SetStateStarted(signature)
	assert.Nil(t, err)
	assert.True(t, isUpdateItemCalled)
	client.ResetOverrides()
}

func TestSetStateRetry(t *testing.T) {
	signature := &tasks.Signature{UUID: "testTaskUUID"}

	// assert correct task ID, state and *no* TTL value is set in SetStateStarted()
	dynamodb.TestDynamoDBBackend.GetConfig().ResultsExpireIn = 2 * 3600 // results should expire after 2 hours (ignored for this state)
	client := dynamodb.TestDynamoDBBackend.GetClient().(*dynamodb.TestDynamoDBClient)
	var isUpdateItemCalled bool
	client.UpdateItemOverride = func(input *awsdynamodb.UpdateItemInput) (*awsdynamodb.UpdateItemOutput, error) {
		isUpdateItemCalled = true
		verifyUpdateInput(t, input, signature.UUID, tasks.StateRetry, time.Time{})
		return &awsdynamodb.UpdateItemOutput{}, nil
	}

	err := dynamodb.TestDynamoDBBackend.SetStateRetry(signature)
	assert.Nil(t, err)
	assert.True(t, isUpdateItemCalled)
	client.ResetOverrides()
}

func TestPrivateFuncGetStates(t *testing.T) {
	task1 := map[string]*awsdynamodb.AttributeValue{
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
	task2 := map[string]*awsdynamodb.AttributeValue{
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
	task3 := map[string]*awsdynamodb.AttributeValue{
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
	dynamodb.TestTask1, dynamodb.TestTask2, dynamodb.TestTask3 = task1, task2, task3

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
	states, err := dynamodb.TestDynamoDBBackend.GetStatesForTest(taskUUIDs...)
	assert.Nil(t, err)

	for _, s := range states {
		assert.EqualValues(t, *s, *expectedStates[s.TaskUUID])
	}
}

func TestGroupTaskStates(t *testing.T) {
	groupUUID := "testGroupUUID"
	count := 3
	task1 := map[string]*awsdynamodb.AttributeValue{
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
	task2 := map[string]*awsdynamodb.AttributeValue{
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
	task3 := map[string]*awsdynamodb.AttributeValue{
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
	dynamodb.TestTask1, dynamodb.TestTask2, dynamodb.TestTask3 = task1, task2, task3
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

	states, err := dynamodb.TestDynamoDBBackend.GroupTaskStates(groupUUID, count)
	assert.Nil(t, err)
	for _, s := range states {
		assert.EqualValues(t, *s, *expectedStates[s.TaskUUID])
	}
}

func TestTriggerChord(t *testing.T) {
	groupUUID := "testGroupUUID"
	triggered, err := dynamodb.TestDynamoDBBackend.TriggerChord(groupUUID)
	assert.Nil(t, err)
	assert.True(t, triggered)
}

func TestGetState(t *testing.T) {
	taskUUID := "testTaskUUID1"
	expectedState := &tasks.TaskState{
		TaskUUID: "testTaskUUID1",
		Results:  nil,
		State:    tasks.StatePending,
		Error:    "",
	}
	state, err := dynamodb.TestDynamoDBBackend.GetState(taskUUID)
	assert.Nil(t, err)
	assert.EqualValues(t, expectedState, state)
}

func TestPurgeState(t *testing.T) {
	taskUUID := "testTaskUUID1"
	err := dynamodb.TestDynamoDBBackend.PurgeState(taskUUID)
	assert.Nil(t, err)

	err = dynamodb.TestErrDynamoDBBackend.PurgeState(taskUUID)
	assert.NotNil(t, err)
}

func TestPurgeGroupMeta(t *testing.T) {
	groupUUID := "GroupUUID"
	err := dynamodb.TestDynamoDBBackend.PurgeGroupMeta(groupUUID)
	assert.Nil(t, err)

	err = dynamodb.TestErrDynamoDBBackend.PurgeGroupMeta(groupUUID)
	assert.NotNil(t, err)
}

func TestPrivateFuncLockGroupMeta(t *testing.T) {
	groupUUID := "GroupUUID"
	err := dynamodb.TestDynamoDBBackend.LockGroupMetaForTest(groupUUID)
	assert.Nil(t, err)
	err = dynamodb.TestErrDynamoDBBackend.LockGroupMetaForTest(groupUUID)
	assert.NotNil(t, err)
}

func TestPrivateFuncUnLockGroupMeta(t *testing.T) {
	groupUUID := "GroupUUID"
	err := dynamodb.TestDynamoDBBackend.UnlockGroupMetaForTest(groupUUID)
	assert.Nil(t, err)
	err = dynamodb.TestErrDynamoDBBackend.UnlockGroupMetaForTest(groupUUID)
	assert.NotNil(t, err)
}

func TestPrivateFuncChordTriggered(t *testing.T) {
	groupUUID := "GroupUUID"
	err := dynamodb.TestDynamoDBBackend.ChordTriggeredForTest(groupUUID)
	assert.Nil(t, err)
	err = dynamodb.TestErrDynamoDBBackend.ChordTriggeredForTest(groupUUID)
	assert.NotNil(t, err)
}

func TestDynamoDBPrivateFuncUpdateGroupMetaLock(t *testing.T) {
	groupUUID := "GroupUUID"
	err := dynamodb.TestDynamoDBBackend.UpdateGroupMetaLockForTest(groupUUID, true)
	assert.Nil(t, err)
	err = dynamodb.TestErrDynamoDBBackend.UpdateGroupMetaLockForTest(groupUUID, true)
	assert.NotNil(t, err)
}

func TestPrivateFuncUpdateToFailureStateWithError(t *testing.T) {
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
	err := dynamodb.TestDynamoDBBackend.UpdateToFailureStateWithErrorForTest(state)
	assert.Nil(t, err)
}

func TestPrivateFuncTableExistsForTest(t *testing.T) {
	tables := []*string{aws.String("foo")}
	assert.False(t, dynamodb.TestDynamoDBBackend.TableExistsForTest("bar", tables))
	assert.True(t, dynamodb.TestDynamoDBBackend.TableExistsForTest("foo", tables))
}

func TestPrivateFuncCheckRequiredTablesIfExistForTest(t *testing.T) {
	err := dynamodb.TestDynamoDBBackend.CheckRequiredTablesIfExistForTest()
	assert.Nil(t, err)
	taskTable := dynamodb.TestDynamoDBBackend.GetConfig().DynamoDB.TaskStatesTable
	groupTable := dynamodb.TestDynamoDBBackend.GetConfig().DynamoDB.GroupMetasTable
	err = dynamodb.TestErrDynamoDBBackend.CheckRequiredTablesIfExistForTest()
	assert.NotNil(t, err)
	dynamodb.TestDynamoDBBackend.GetConfig().DynamoDB.TaskStatesTable = "foo"
	err = dynamodb.TestDynamoDBBackend.CheckRequiredTablesIfExistForTest()
	assert.NotNil(t, err)
	dynamodb.TestDynamoDBBackend.GetConfig().DynamoDB.TaskStatesTable = taskTable
	dynamodb.TestDynamoDBBackend.GetConfig().DynamoDB.GroupMetasTable = "foo"
	err = dynamodb.TestDynamoDBBackend.CheckRequiredTablesIfExistForTest()
	assert.NotNil(t, err)
	dynamodb.TestDynamoDBBackend.GetConfig().DynamoDB.GroupMetasTable = groupTable
}
