package backends

import (
	"errors"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"time"
)

type DynamoDBBackend struct {
	cnf     *config.Config
	client  dynamodbiface.DynamoDBAPI
	session *session.Session
}

const DBTableTaskStates = "task_states"
const DBTableGroupMeta = "group_meta"

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
		TableName: aws.String(DBTableGroupMeta),
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

// todo: using curent function like newPendingTaskState
func (b *DynamoDBBackend) SetStatePending(signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	return b.setTaskState(taskState)
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
		TableName: aws.String(DBTableTaskStates),
		Key: map[string]*dynamodb.AttributeValue{
			"TaskUUID": {
				S: aws.String(taskUUID),
			},
		},
	})
	log.INFO.Println(result)

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
		TableName: aws.String(DBTableTaskStates),
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
		TableName: aws.String(DBTableGroupMeta),
	}
	_, err := b.client.DeleteItem(input)

	if err != nil {
		return err
	}
	return nil
}

func (b *DynamoDBBackend) getGroupMeta(groupUUID string) (*tasks.GroupMeta, error) {
	result, err := b.client.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(DBTableGroupMeta),
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
		log.ERROR.Printf("Failed to unmarshal item, %v", err)
		return nil, err
	}
	return item, nil
}

func (b *DynamoDBBackend) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	states := make([]*tasks.TaskState, 0)

	filt := expression.Name("TaskUUID").In(expression.Value(taskUUIDs))
	proj := expression.NamesList(expression.Name("State"), expression.Name("Results"), expression.Name("Error"))
	expr, err := expression.NewBuilder().WithFilter(filt).WithProjection(proj).Build()
	if err != nil {
		return nil, err
	}
	params := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String(DBTableTaskStates),
	}

	// Make the DynamoDB Query API call
	result, err := b.client.Scan(params)
	if err != nil {
		log.ERROR.Printf("Error when calling DynamoDB scan API. Err: %v", err)
		return nil, err
	}
	for _, i := range result.Items {
		state := tasks.TaskState{}
		err = dynamodbattribute.UnmarshalMap(i, &state)
		if err != nil {
			log.ERROR.Printf("Got error unmarshalling. Error: %v", err)
			return nil, err
		}
		states = append(states, &state)
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
		TableName:        aws.String(DBTableGroupMeta),
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
		TableName:        aws.String(DBTableGroupMeta),
		UpdateExpression: aws.String("SET #CT = :ct"),
	}

	_, err := b.client.UpdateItem(input)

	if err != nil {
		return err
	}
	return nil
}

func (b *DynamoDBBackend) setTaskState(taskState *tasks.TaskState) error {
	attributeNames := map[string]*string{
		"#S": aws.String("State"),
	}
	attributeValues := map[string]*dynamodb.AttributeValue{
		":s": {
			S: aws.String(taskState.State),
		},
	}
	exp := "SET #S = :s"
	if !taskState.CreatedAt.IsZero() {
		attributeNames["#C"] = aws.String("CreatedAt")
		attributeValues[":c"] = &dynamodb.AttributeValue{
			S: aws.String(taskState.CreatedAt.String()),
		}
		exp += ", #C = :c"
	}
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames: attributeNames,
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":s": {
				S: aws.String(taskState.State),
			},
		},
		Key:              attributeValues,
		ReturnValues:     aws.String("UPDATED_NEW"),
		TableName:        aws.String(DBTableTaskStates),
		UpdateExpression: aws.String(exp),
	}

	// UpdateItem will create an item if there is no record have been found
	_, err := b.client.UpdateItem(input)

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
		TableName:        aws.String(DBTableTaskStates),
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
