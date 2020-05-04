package dynamodb_test

import (
	"fmt"
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
	client := dynamodb.TestDynamoDBBackend.GetClient().(*dynamodb.TestDynamoDBClient)
	tableName := dynamodb.TestDynamoDBBackend.GetConfig().DynamoDB.TaskStatesTable
	// Override DynamoDB BatchGetItem() behavior
	var isBatchGetItemCalled bool
	client.BatchGetItemOverride = func(input *awsdynamodb.BatchGetItemInput) (*awsdynamodb.BatchGetItemOutput, error) {
		isBatchGetItemCalled = true
		assert.NotNil(t, input)
		assert.Nil(t, input.Validate())

		return &awsdynamodb.BatchGetItemOutput{
			Responses: map[string][]map[string]*awsdynamodb.AttributeValue{
				tableName: {
					{"State": {S: aws.String(tasks.StateSuccess)}},
					{"State": {S: aws.String(tasks.StateSuccess)}},
					{"State": {S: aws.String(tasks.StateFailure)}},
				},
			},
		}, nil
	}
	groupUUID := "testGroupUUID"
	isCompleted, err := dynamodb.TestDynamoDBBackend.GroupCompleted(groupUUID, 3)
	assert.Nil(t, err)
	assert.True(t, isCompleted)
	assert.True(t, isBatchGetItemCalled)
	client.ResetOverrides()
}
func TestGroupCompletedReturnsError(t *testing.T) {
	client := dynamodb.TestDynamoDBBackend.GetClient().(*dynamodb.TestDynamoDBClient)
	client.BatchGetItemOverride = func(input *awsdynamodb.BatchGetItemInput) (*awsdynamodb.BatchGetItemOutput, error) {
		return nil, fmt.Errorf("Simulating error from AWS")
	}
	isCompleted, err := dynamodb.TestDynamoDBBackend.GroupCompleted("test", 3)
	assert.NotNil(t, err)
	assert.False(t, isCompleted)
	client.ResetOverrides()
}

// TestGroupCompletedReturnsFalse tests that the GroupCompleted() returns false when some tasks have not yet finished.
func TestGroupCompletedReturnsFalse(t *testing.T) {
	client := dynamodb.TestDynamoDBBackend.GetClient().(*dynamodb.TestDynamoDBClient)
	tableName := dynamodb.TestDynamoDBBackend.GetConfig().DynamoDB.TaskStatesTable
	// Override DynamoDB BatchGetItem() behavior
	client.BatchGetItemOverride = func(_ *awsdynamodb.BatchGetItemInput) (*awsdynamodb.BatchGetItemOutput, error) {
		return &awsdynamodb.BatchGetItemOutput{
			Responses: map[string][]map[string]*awsdynamodb.AttributeValue{
				tableName: {
					{"State": {S: aws.String(tasks.StateSuccess)}},
					{"State": {S: aws.String(tasks.StateFailure)}},
					{"State": {S: aws.String(tasks.StatePending)}},
				},
			},
		}, nil
	}
	isCompleted, err := dynamodb.TestDynamoDBBackend.GroupCompleted("testGroup", 3)
	assert.Nil(t, err)
	assert.False(t, isCompleted)
	client.ResetOverrides()
}

// TestGroupCompletedReturnsFalse tests that the GroupCompleted() retries the the request until MaxFetchAttempts before returning an error
func TestGroupCompletedRetries(t *testing.T) {
	client := dynamodb.TestDynamoDBBackend.GetClient().(*dynamodb.TestDynamoDBClient)
	tableName := dynamodb.TestDynamoDBBackend.GetConfig().DynamoDB.TaskStatesTable
	// Override DynamoDB BatchGetItem() behavior
	var countBatchGetItemAPICalls int
	client.BatchGetItemOverride = func(_ *awsdynamodb.BatchGetItemInput) (*awsdynamodb.BatchGetItemOutput, error) {
		countBatchGetItemAPICalls++

		return &awsdynamodb.BatchGetItemOutput{
			Responses: map[string][]map[string]*awsdynamodb.AttributeValue{
				tableName: {
					{"State": {S: aws.String(tasks.StateSuccess)}},
				},
			},
			UnprocessedKeys: map[string]*awsdynamodb.KeysAndAttributes{
				tableName: {
					Keys: []map[string]*awsdynamodb.AttributeValue{
						{"TaskUUID": {S: aws.String("unfetchedTaskUUID1")}},
						{"TaskUUID": {S: aws.String("unfetchedTaskUUID2")}},
					},
				},
			},
		}, nil
	}
	_, err := dynamodb.TestDynamoDBBackend.GroupCompleted("testGroup", 3)
	assert.NotNil(t, err)
	assert.Equal(t, dynamodb.MaxFetchAttempts, countBatchGetItemAPICalls)
	client.ResetOverrides()
}

// TestGroupCompletedReturnsFalse tests that the GroupCompleted() retries the the request and returns success if all keys are fetched on retries.
func TestGroupCompletedRetrieSuccess(t *testing.T) {
	client := dynamodb.TestDynamoDBBackend.GetClient().(*dynamodb.TestDynamoDBClient)
	tableName := dynamodb.TestDynamoDBBackend.GetConfig().DynamoDB.TaskStatesTable
	// Override DynamoDB BatchGetItem() behavior
	var countBatchGetItemAPICalls int
	client.BatchGetItemOverride = func(_ *awsdynamodb.BatchGetItemInput) (*awsdynamodb.BatchGetItemOutput, error) {
		countBatchGetItemAPICalls++

		// simulate unfetched keys on 1st attempt.
		if countBatchGetItemAPICalls == 1 {
			return &awsdynamodb.BatchGetItemOutput{
				Responses: map[string][]map[string]*awsdynamodb.AttributeValue{
					tableName: {}, // no keys returned in this attempt.
				},
				UnprocessedKeys: map[string]*awsdynamodb.KeysAndAttributes{
					tableName: {
						Keys: []map[string]*awsdynamodb.AttributeValue{
							{"TaskUUID": {S: aws.String("unfetchedTaskUUID1")}},
							{"TaskUUID": {S: aws.String("unfetchedTaskUUID2")}},
							{"TaskUUID": {S: aws.String("unfetchedTaskUUID3")}},
						},
					},
				},
			}, nil

		}

		// Return all keys in subsequent attempts.
		return &awsdynamodb.BatchGetItemOutput{
			Responses: map[string][]map[string]*awsdynamodb.AttributeValue{
				tableName: {
					{"State": {S: aws.String(tasks.StateSuccess)}},
					{"State": {S: aws.String(tasks.StateSuccess)}},
					{"State": {S: aws.String(tasks.StateSuccess)}},
				},
			},
		}, nil
	}
	isCompleted, err := dynamodb.TestDynamoDBBackend.GroupCompleted("testGroup", 3)
	assert.Nil(t, err)
	assert.True(t, isCompleted)
	assert.Equal(t, 2, countBatchGetItemAPICalls)
	client.ResetOverrides()
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

func TestGroupTaskStates(t *testing.T) {
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
	client := dynamodb.TestDynamoDBBackend.GetClient().(*dynamodb.TestDynamoDBClient)
	tableName := dynamodb.TestDynamoDBBackend.GetConfig().DynamoDB.TaskStatesTable
	client.BatchGetItemOverride = func(input *awsdynamodb.BatchGetItemInput) (*awsdynamodb.BatchGetItemOutput, error) {
		assert.Nil(t, input.Validate())
		return &awsdynamodb.BatchGetItemOutput{
			Responses: map[string][]map[string]*awsdynamodb.AttributeValue{
				tableName: {
					{
						"TaskUUID": {S: aws.String("testTaskUUID1")},
						"Results:": {NULL: aws.Bool(true)},
						"State":    {S: aws.String(tasks.StatePending)},
						"Error":    {NULL: aws.Bool(true)},
					},
					{
						"TaskUUID": {S: aws.String("testTaskUUID2")},
						"Results:": {NULL: aws.Bool(true)},
						"State":    {S: aws.String(tasks.StateStarted)},
						"Error":    {NULL: aws.Bool(true)},
					},
					{
						"TaskUUID": {S: aws.String("testTaskUUID3")},
						"Results:": {NULL: aws.Bool(true)},
						"State":    {S: aws.String(tasks.StateSuccess)},
						"Error":    {NULL: aws.Bool(true)},
					},
				},
			},
		}, nil
	}
	defer client.ResetOverrides()

	states, err := dynamodb.TestDynamoDBBackend.GroupTaskStates("testGroupUUID", 3)
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
	client := dynamodb.TestDynamoDBBackend.GetClient().(*dynamodb.TestDynamoDBClient)
	client.GetItemOverride = func(input *awsdynamodb.GetItemInput) (*awsdynamodb.GetItemOutput, error) {
		return &awsdynamodb.GetItemOutput{
			Item: map[string]*awsdynamodb.AttributeValue{
				"TaskUUID": {S: aws.String("testTaskUUID1")},
				"Results:": {NULL: aws.Bool(true)},
				"State":    {S: aws.String(tasks.StatePending)},
				"Error":    {NULL: aws.Bool(false)},
			},
		}, nil
	}
	defer client.ResetOverrides()

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
