package dynamodb_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/backends/dynamodb"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	awsdynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestNew(t *testing.T) {
	// should call t.Skip if not connected to internet
	backend := dynamodb.New(dynamodb.TestCnf)
	assert.IsType(t, new(dynamodb.Backend), backend)
}

func TestInitGroup(t *testing.T) {
	groupUUID := uuid.New().String()
	taskUUIDs := []string{uuid.New().String(), uuid.New().String(), uuid.New().String()}
	log.INFO.Println(dynamodb.TestDynamoDBBackend.GetConfig())

	err := dynamodb.TestDynamoDBBackend.InitGroup(groupUUID, taskUUIDs)
	assert.NoError(t, err)

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
	assert.NoError(t, err)
	assert.True(t, isPutItemCalled)
	client.ResetOverrides()
}

func assertTTLValue(t *testing.T, expectedTTLTime time.Time, actualEncodedTTLValue string) {
	actualTTLTimestamp, err := strconv.ParseInt(actualEncodedTTLValue, 10, 64)
	assert.NoError(t, err)
	actualTTLTime := time.Unix(actualTTLTimestamp, 0)
	assert.WithinDuration(t, expectedTTLTime, actualTTLTime, time.Second)
}

func TestGroupCompleted(t *testing.T) {
	groupUUID, _ := createGroupWithSucceedTask([]string{tasks.StateSuccess, tasks.StateSuccess}, t)

	isCompleted, err := dynamodb.TestDynamoDBBackend.GroupCompleted(groupUUID, 2)
	assert.NoError(t, err)
	assert.True(t, isCompleted)
}

// TestGroupCompletedReturnsFalse tests that the GroupCompleted() returns false when some tasks have not yet finished.
func TestGroupCompletedReturnsFalse(t *testing.T) {
	groupUUID, _ := createGroupWithSucceedTask([]string{tasks.StateSuccess, tasks.StateSuccess, tasks.StatePending}, t)

	isCompleted, err := dynamodb.TestDynamoDBBackend.GroupCompleted(groupUUID, 3)
	assert.NoError(t, err)
	assert.False(t, isCompleted)
}

func createGroupWithSucceedTask(taskStates []string, t *testing.T) (string, []string) {
	groupUUID := uuid.New().String()
	taskUUIDs := make([]string, 0, len(taskStates))
	for i := 0; i < len(taskStates); i++ {
		taskUUIDs = append(taskUUIDs, uuid.New().String())

		switch taskStates[i] {
		case tasks.StateSuccess:
			err := dynamodb.TestDynamoDBBackend.SetStateSuccess(&tasks.Signature{
				UUID: taskUUIDs[i],
			}, []*tasks.TaskResult{})
			assert.NoError(t, err)
		case tasks.StateFailure:
			err := dynamodb.TestDynamoDBBackend.SetStateFailure(&tasks.Signature{
				UUID: taskUUIDs[i]}, "unexpected failure")
			assert.NoError(t, err)
		case tasks.StatePending:
			err := dynamodb.TestDynamoDBBackend.SetStatePending(&tasks.Signature{
				UUID: taskUUIDs[i],
			})
			assert.NoError(t, err)
		case tasks.StateStarted:
			err := dynamodb.TestDynamoDBBackend.SetStateStarted(&tasks.Signature{
				UUID: taskUUIDs[i],
			})
			assert.NoError(t, err)
		}

	}
	log.INFO.Println(dynamodb.TestDynamoDBBackend.GetConfig())
	err := dynamodb.TestDynamoDBBackend.InitGroup(groupUUID, taskUUIDs)
	assert.NoError(t, err)

	return groupUUID, taskUUIDs
}

// TestGroupCompletedReturnsFalse tests that the GroupCompleted() retries the the request until MaxFetchAttempts before returning an error
func TestGroupCompletedRetries(t *testing.T) {
	groupUUID, taskUUIDs := createGroupWithSucceedTask([]string{tasks.StateSuccess, tasks.StateSuccess}, t)

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
						{"TaskUUID": {S: aws.String(taskUUIDs[0])}},
						{"TaskUUID": {S: aws.String(taskUUIDs[1])}},
					},
				},
			},
		}, nil
	}

	_, err := dynamodb.TestDynamoDBBackend.GroupCompleted(groupUUID, 3)
	assert.Error(t, err)
	assert.Equal(t, dynamodb.MaxFetchAttempts, countBatchGetItemAPICalls)
	client.ResetOverrides()
}

// TestGroupCompletedReturnsFalse tests that the GroupCompleted() retries the the request and returns success if all keys are fetched on retries.
func TestGroupCompletedRetrieSuccess(t *testing.T) {
	groupID, taskUUIDs := createGroupWithSucceedTask([]string{tasks.StateSuccess, tasks.StateSuccess, tasks.StateSuccess}, t)
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
							{"TaskUUID": {S: aws.String(taskUUIDs[0])}},
							{"TaskUUID": {S: aws.String(taskUUIDs[1])}},
							{"TaskUUID": {S: aws.String(taskUUIDs[2])}},
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
	isCompleted, err := dynamodb.TestDynamoDBBackend.GroupCompleted(groupID, 3)
	assert.NoError(t, err)
	assert.True(t, isCompleted)
	assert.Equal(t, 2, countBatchGetItemAPICalls)
	client.ResetOverrides()
}

func TestPrivateFuncGetGroupMeta(t *testing.T) {
	groupUUID, taskUUIDs := createGroupWithSucceedTask([]string{tasks.StateSuccess, tasks.StateSuccess, tasks.StateSuccess}, t)
	meta, err := dynamodb.TestDynamoDBBackend.GetGroupMetaForTest(groupUUID)
	assert.NoError(t, err)

	item := tasks.GroupMeta{
		GroupUUID:      groupUUID,
		Lock:           false,
		ChordTriggered: false,
		TaskUUIDs:      taskUUIDs,
		CreatedAt:      meta.CreatedAt,
		TTL:            meta.TTL,
	}
	assert.EqualValues(t, item, *meta)
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
	state, err := dynamodb.TestDynamoDBBackend.UnmarshalTaskStateGetItemResultForTest(&result)
	assert.NoError(t, err)
	assert.EqualValues(t, item, *state)

	_, err = dynamodb.TestDynamoDBBackend.UnmarshalTaskStateGetItemResultForTest(nil)
	assert.Error(t, err)

	_, err = dynamodb.TestDynamoDBBackend.UnmarshalTaskStateGetItemResultForTest(&invalidResult)
	assert.Error(t, err)

}

func TestPrivateFuncSetTaskState(t *testing.T) {
	signature := &tasks.Signature{
		Name: "Test",
		UUID: uuid.New().String(),
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: 1,
			},
		},
	}
	state := tasks.NewPendingTaskState(signature)
	err := dynamodb.TestDynamoDBBackend.SetTaskStateForTest(state)
	assert.NoError(t, err)
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
	signature := &tasks.Signature{UUID: uuid.New().String()}

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
	assert.NoError(t, err)
	assert.True(t, isUpdateItemCalled)
	client.ResetOverrides()
}

func TestSetStateFailure(t *testing.T) {
	signature := &tasks.Signature{UUID: uuid.New().String()}

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
	assert.NoError(t, err)
	assert.True(t, isUpdateItemCalled)
	client.ResetOverrides()
}

func TestSetStateReceived(t *testing.T) {
	signature := &tasks.Signature{UUID: uuid.New().String()}

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
	assert.NoError(t, err)
	assert.True(t, isUpdateItemCalled)
	client.ResetOverrides()
}

func TestSetStateStarted(t *testing.T) {
	signature := &tasks.Signature{UUID: uuid.New().String()}

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
	assert.NoError(t, err)
	assert.True(t, isUpdateItemCalled)
	client.ResetOverrides()
}

func TestSetStateRetry(t *testing.T) {
	signature := &tasks.Signature{UUID: uuid.New().String()}

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
	assert.NoError(t, err)
	assert.True(t, isUpdateItemCalled)
	client.ResetOverrides()
}

func TestGroupTaskStates(t *testing.T) {
	groupUUID, taskUUIDs := createGroupWithSucceedTask([]string{tasks.StatePending, tasks.StateStarted, tasks.StateSuccess}, t)
	expectedStates := map[string]*tasks.TaskState{
		taskUUIDs[0]: {
			TaskUUID: taskUUIDs[0],
			Results:  nil,
			State:    tasks.StatePending,
			Error:    "",
		},
		taskUUIDs[1]: {
			TaskUUID: taskUUIDs[1],
			Results:  nil,
			State:    tasks.StateStarted,
			Error:    "",
		},
		taskUUIDs[2]: {
			TaskUUID: taskUUIDs[2],
			Results:  nil,
			State:    tasks.StateSuccess,
			Error:    "",
		},
	}

	states, err := dynamodb.TestDynamoDBBackend.GroupTaskStates(groupUUID, 3)
	assert.NoError(t, err)
	for _, s := range states {
		expectedStates[s.TaskUUID].TTL = s.TTL
		expectedStates[s.TaskUUID].CreatedAt = s.CreatedAt
		assert.EqualValues(t, *s, *expectedStates[s.TaskUUID])
	}
}

func TestTriggerChord(t *testing.T) {
	groupUUID, _ := createGroupWithSucceedTask([]string{tasks.StateSuccess, tasks.StateSuccess, tasks.StateSuccess}, t)
	triggered, err := dynamodb.TestDynamoDBBackend.TriggerChord(groupUUID)
	assert.NoError(t, err)
	assert.True(t, triggered)
}

func TestGetState(t *testing.T) {
	taskUUID := uuid.New().String()
	err := dynamodb.TestDynamoDBBackend.SetStatePending(&tasks.Signature{
		UUID: taskUUID,
	})

	state, err := dynamodb.TestDynamoDBBackend.GetState(taskUUID)
	assert.NoError(t, err)

	expectedState := &tasks.TaskState{
		TaskUUID:  taskUUID,
		Results:   nil,
		State:     tasks.StatePending,
		Error:     "",
		CreatedAt: state.CreatedAt,
	}
	assert.EqualValues(t, expectedState, state)
}

func TestPurgeState(t *testing.T) {
	_, taskUUIDs := createGroupWithSucceedTask([]string{tasks.StateSuccess}, t)
	err := dynamodb.TestDynamoDBBackend.PurgeState(taskUUIDs[0])
	assert.NoError(t, err)
}

func TestPurgeGroupMeta(t *testing.T) {
	groupUUID, _ := createGroupWithSucceedTask([]string{tasks.StateSuccess}, t)
	err := dynamodb.TestDynamoDBBackend.PurgeGroupMeta(groupUUID)
	assert.NoError(t, err)
}

func TestPrivateFuncLockGroupMeta(t *testing.T) {
	groupUUID, _ := createGroupWithSucceedTask([]string{tasks.StateSuccess}, t)
	err := dynamodb.TestDynamoDBBackend.LockGroupMetaForTest(groupUUID)
	assert.NoError(t, err)
}

func TestPrivateFuncUnLockGroupMeta(t *testing.T) {
	groupUUID := "GroupUUID"
	err := dynamodb.TestDynamoDBBackend.UnlockGroupMetaForTest(groupUUID)
	assert.NoError(t, err)
}

func TestPrivateFuncChordTriggered(t *testing.T) {
	groupUUID, _ := createGroupWithSucceedTask([]string{tasks.StateSuccess}, t)
	err := dynamodb.TestDynamoDBBackend.ChordTriggeredForTest(groupUUID)
	assert.NoError(t, err)
}

func TestDynamoDBPrivateFuncUpdateGroupMetaLock(t *testing.T) {
	groupUUID, _ := createGroupWithSucceedTask([]string{tasks.StateSuccess}, t)
	err := dynamodb.TestDynamoDBBackend.UpdateGroupMetaLockForTest(groupUUID, true)
	assert.NoError(t, err)
}

func TestPrivateFuncUpdateToFailureStateWithError(t *testing.T) {
	signature := &tasks.Signature{
		Name: "Test",
		UUID: uuid.New().String(),
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: 1,
			},
		},
	}

	state := tasks.NewFailureTaskState(signature, "This is an error")
	err := dynamodb.TestDynamoDBBackend.UpdateToFailureStateWithErrorForTest(state)
	assert.NoError(t, err)
}

func TestPrivateFuncTableExistsForTest(t *testing.T) {
	tables := []*string{aws.String("foo")}
	assert.False(t, dynamodb.TestDynamoDBBackend.TableExistsForTest("bar", tables))
	assert.True(t, dynamodb.TestDynamoDBBackend.TableExistsForTest("foo", tables))
}

func TestPrivateFuncCheckRequiredTablesIfExistForTest(t *testing.T) {
	err := dynamodb.TestDynamoDBBackend.CheckRequiredTablesIfExistForTest()
	assert.NoError(t, err)

	dynamodb.TestDynamoDBBackend.GetConfig().DynamoDB.TaskStatesTable = "foo"
	err = dynamodb.TestDynamoDBBackend.CheckRequiredTablesIfExistForTest()
	assert.Error(t, err)

	taskTable := dynamodb.TestDynamoDBBackend.GetConfig().DynamoDB.TaskStatesTable
	groupTable := dynamodb.TestDynamoDBBackend.GetConfig().DynamoDB.GroupMetasTable
	dynamodb.TestDynamoDBBackend.GetConfig().DynamoDB.TaskStatesTable = taskTable
	dynamodb.TestDynamoDBBackend.GetConfig().DynamoDB.GroupMetasTable = "foo"
	err = dynamodb.TestDynamoDBBackend.CheckRequiredTablesIfExistForTest()
	assert.Error(t, err)

	dynamodb.TestDynamoDBBackend.GetConfig().DynamoDB.GroupMetasTable = groupTable
}
