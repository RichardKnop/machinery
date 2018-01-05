package backends

import (
	"errors"
	"fmt"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type DynamoDBBackend struct {
	cnf     *config.Config
	client  dynamodbiface.DynamoDBAPI
	session *session.Session
}

// NewDynamoDBBackend creates a DynamoDBBackend instance
func NewDynamoDBBackend(cnf *config.Config) Interface {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	dy := dynamodb.New(sess)
	return &DynamoDBBackend{cnf: cnf, client: dy, session: sess}
}

func (b *DynamoDBBackend) InitGroup(groupUUID string, taskUUIDs []string) error {
	meta := tasks.GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
		CreatedAt: time.Now().UTC(),
	}
	av, err := dynamodbattribute.MarshalMap(meta)
	if err != nil {
		log.ERROR.Printf("Error when marshaling Dynamodb attributes. Err: %v", err)
		return err
	}
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(b.cnf.DynamoDB.GroupMetasTable),
	}
	_, err = b.client.PutItem(input)

	if err != nil {
		log.ERROR.Printf("Got error when calling PutItem: %v; Error: %v", input, err)
		return err
	}
	return nil
}

func (b *DynamoDBBackend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}
	taskStates, err := b.getStates(groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}
	var countSuccessTasks = 0
	for _, taskState := range taskStates {
		if taskState.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

func (b *DynamoDBBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return nil, err
	}

	return b.getStates(groupMeta.TaskUUIDs...)
}

func (b *DynamoDBBackend) TriggerChord(groupUUID string) (bool, error) {
	// Get the group meta data
	groupMeta, err := b.getGroupMeta(groupUUID)

	if err != nil {
		return false, err
	}

	// Chord has already been triggered, return false (should not trigger again)
	if groupMeta.ChordTriggered {
		return false, nil
	}

	// If group meta is locked, wait until it's unlocked
	for groupMeta.Lock {
		groupMeta, _ = b.getGroupMeta(groupUUID)
		log.WARNING.Print("Group meta locked, waiting")
		<-time.After(time.Millisecond * 5)
	}

	// Acquire lock
	if err = b.lockGroupMeta(groupUUID); err != nil {
		return false, err
	}
	defer b.unlockGroupMeta(groupUUID)

	// update group meta data
	err = b.chordTriggered(groupUUID)
	if err != nil {
		return false, err
	}
	return true, err
}

func (b *DynamoDBBackend) SetStatePending(signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	// taskUUID is the primary key of the table, so a new task need to be created first, instead of using dynamodb.UpdateItemInput directly
	return b.initTaskState(taskState)
}

func (b *DynamoDBBackend) SetStateReceived(signature *tasks.Signature) error {
	taskState := tasks.NewReceivedTaskState(signature)
	return b.setTaskState(taskState)
}

func (b *DynamoDBBackend) SetStateStarted(signature *tasks.Signature) error {
	taskState := tasks.NewStartedTaskState(signature)
	return b.setTaskState(taskState)
}

func (b *DynamoDBBackend) SetStateRetry(signature *tasks.Signature) error {
	taskState := tasks.NewRetryTaskState(signature)
	return b.setTaskState(taskState)
}

func (b *DynamoDBBackend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	taskState := tasks.NewSuccessTaskState(signature, results)
	return b.setTaskState(taskState)
}

func (b *DynamoDBBackend) SetStateFailure(signature *tasks.Signature, err string) error {
	taskState := tasks.NewFailureTaskState(signature, err)
	return b.updateToFailureStateWithError(taskState)
}

func (b *DynamoDBBackend) GetState(taskUUID string) (*tasks.TaskState, error) {
	result, err := b.client.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(b.cnf.DynamoDB.TaskStatesTable),
		Key: map[string]*dynamodb.AttributeValue{
			"TaskUUID": {
				S: aws.String(taskUUID),
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return b.unmarshalTaskStateGetItemResult(result)
}

func (b *DynamoDBBackend) PurgeState(taskUUID string) error {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"TaskUUID": {
				N: aws.String(taskUUID),
			},
		},
		TableName: aws.String(b.cnf.DynamoDB.TaskStatesTable),
	}
	_, err := b.client.DeleteItem(input)

	if err != nil {
		return err
	}
	return nil
}

func (b *DynamoDBBackend) PurgeGroupMeta(groupUUID string) error {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"GroupUUID": {
				N: aws.String(groupUUID),
			},
		},
		TableName: aws.String(b.cnf.DynamoDB.GroupMetasTable),
	}
	_, err := b.client.DeleteItem(input)

	if err != nil {
		return err
	}
	return nil
}

func (b *DynamoDBBackend) getGroupMeta(groupUUID string) (*tasks.GroupMeta, error) {
	result, err := b.client.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(b.cnf.DynamoDB.GroupMetasTable),
		Key: map[string]*dynamodb.AttributeValue{
			"GroupUUID": {
				S: aws.String(groupUUID),
			},
		},
	})
	if err != nil {
		log.ERROR.Printf("Error when getting group meta. Error: %v", err)
		return nil, err
	}
	item, err := b.unmarshalGroupMetaGetItemResult(result)
	if err != nil {
		log.INFO.Println("!!!", result)
		log.ERROR.Printf("Failed to unmarshal item, %v", err)
		return nil, err
	}
	return item, nil
}

func (b *DynamoDBBackend) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	states := make([]*tasks.TaskState, 0)
	stateChan := make(chan *tasks.TaskState, len(taskUUIDs))
	errChan := make(chan error)
	// There is no method like querying items by `in` a list of primary keys.
	// So a for loop with go routine is used to get multiple items
	for _, id := range taskUUIDs {
		go func(id string) {
			state, err := b.GetState(id)
			if err != nil {
				errChan <- err
			}
			stateChan <- state
		}(id)
	}

	for s := range stateChan {
		states = append(states, s)
		if len(states) == len(taskUUIDs) {
			close(stateChan)
		}
	}
	return states, nil
}

func (b *DynamoDBBackend) lockGroupMeta(groupUUID string) error {
	err := b.updateGroupMetaLock(groupUUID, true)
	if err != nil {
		return err
	}
	return nil
}

func (b *DynamoDBBackend) unlockGroupMeta(groupUUID string) error {
	err := b.updateGroupMetaLock(groupUUID, false)
	if err != nil {
		return err
	}
	return nil
}

func (b *DynamoDBBackend) updateGroupMetaLock(groupUUID string, status bool) error {
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames: map[string]*string{
			"#L": aws.String("Lock"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":l": {
				BOOL: aws.Bool(status),
			},
		},
		Key: map[string]*dynamodb.AttributeValue{
			"GroupUUID": {
				S: aws.String(groupUUID),
			},
		},
		ReturnValues:     aws.String("UPDATED_NEW"),
		TableName:        aws.String(b.cnf.DynamoDB.GroupMetasTable),
		UpdateExpression: aws.String("SET #L = :l"),
	}

	_, err := b.client.UpdateItem(input)

	if err != nil {
		return err
	}
	return nil
}

func (b *DynamoDBBackend) chordTriggered(groupUUID string) error {
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames: map[string]*string{
			"#CT": aws.String("ChordTriggered"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":ct": {
				BOOL: aws.Bool(true),
			},
		},
		Key: map[string]*dynamodb.AttributeValue{
			"GroupUUID": {
				S: aws.String(groupUUID),
			},
		},
		ReturnValues:     aws.String("UPDATED_NEW"),
		TableName:        aws.String(b.cnf.DynamoDB.GroupMetasTable),
		UpdateExpression: aws.String("SET #CT = :ct"),
	}

	_, err := b.client.UpdateItem(input)

	if err != nil {
		return err
	}
	return nil
}

func (b *DynamoDBBackend) setTaskState(taskState *tasks.TaskState) error {
	expAttributeNames := map[string]*string{
		"#S": aws.String("State"),
	}
	expAttributeValues := map[string]*dynamodb.AttributeValue{
		":s": {
			S: aws.String(taskState.State),
		},
	}
	keyAttributeValues := map[string]*dynamodb.AttributeValue{
		"TaskUUID": {
			S: aws.String(taskState.TaskUUID),
		},
	}
	exp := "SET #S = :s"
	if !taskState.CreatedAt.IsZero() {
		expAttributeNames["#C"] = aws.String("CreatedAt")
		expAttributeValues[":c"] = &dynamodb.AttributeValue{
			S: aws.String(taskState.CreatedAt.String()),
		}
		exp += ", #C = :c"
	}
	if taskState.Results != nil {
		expAttributeNames["#R"] = aws.String("Results")
		var results []*dynamodb.AttributeValue
		for _, r := range taskState.Results {
			avMap := map[string]*dynamodb.AttributeValue{
				"Type": {
					S: aws.String(r.Type),
				},
				"Value": {
					S: aws.String(fmt.Sprintf("%v", r.Value)),
				},
			}
			rs := &dynamodb.AttributeValue{
				M: avMap,
			}
			results = append(results, rs)
		}
		expAttributeValues[":r"] = &dynamodb.AttributeValue{
			L: results,
		}
		exp += ", #R = :r"
	}
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames:  expAttributeNames,
		ExpressionAttributeValues: expAttributeValues,
		Key:              keyAttributeValues,
		ReturnValues:     aws.String("UPDATED_NEW"),
		TableName:        aws.String(b.cnf.DynamoDB.TaskStatesTable),
		UpdateExpression: aws.String(exp),
	}

	_, err := b.client.UpdateItem(input)

	if err != nil {
		return err
	}
	return nil
}

func (b *DynamoDBBackend) initTaskState(taskState *tasks.TaskState) error {
	av, err := dynamodbattribute.MarshalMap(taskState)
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(b.cnf.DynamoDB.TaskStatesTable),
	}
	_, err = b.client.PutItem(input)

	if err != nil {
		return err
	}
	return nil
}

func (b *DynamoDBBackend) updateToFailureStateWithError(taskState *tasks.TaskState) error {

	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames: map[string]*string{
			"#S": aws.String("State"),
			"#E": aws.String("Error"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":s": {
				S: aws.String(taskState.State),
			},
			":e": {
				S: aws.String(taskState.Error),
			},
		},
		Key: map[string]*dynamodb.AttributeValue{
			"TaskUUID": {
				S: aws.String(taskState.TaskUUID),
			},
		},
		ReturnValues:     aws.String("UPDATED_NEW"),
		TableName:        aws.String(b.cnf.DynamoDB.TaskStatesTable),
		UpdateExpression: aws.String("SET #S = :s, #E = :e"),
	}

	_, err := b.client.UpdateItem(input)

	if err != nil {
		return err
	}
	return nil
}

func (b *DynamoDBBackend) unmarshalGroupMetaGetItemResult(result *dynamodb.GetItemOutput) (*tasks.GroupMeta, error) {
	if result == nil {
		err := errors.New("task state is nil")
		log.ERROR.Printf("Got error when unmarshal map. Error: %v", err)
		return nil, err
	}
	item := tasks.GroupMeta{}
	err := dynamodbattribute.UnmarshalMap(result.Item, &item)
	if err != nil {
		log.ERROR.Printf("Got error when unmarshal map. Error: %v", err)
		return nil, err
	}
	return &item, err
}

func (b *DynamoDBBackend) unmarshalTaskStateGetItemResult(result *dynamodb.GetItemOutput) (*tasks.TaskState, error) {
	if result == nil {
		err := errors.New("task state is nil")
		log.ERROR.Printf("Got error when unmarshal map. Error: %v", err)
		return nil, err
	}
	state := tasks.TaskState{}
	err := dynamodbattribute.UnmarshalMap(result.Item, &state)
	if err != nil {
		log.ERROR.Printf("Got error when unmarshal map. Error: %v", err)
		return nil, err
	}
	return &state, nil
}
