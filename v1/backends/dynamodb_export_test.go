package backends

import (
	"errors"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"os"
)

var (
	TestDynamoDBBackend         *DynamoDBBackend
	TestErrDynamoDBBackend      *DynamoDBBackend
	TestCnf                     *config.Config
	TestSession                 *session.Session
	TestDBClient                dynamodbiface.DynamoDBAPI
	TestErrDBClient             dynamodbiface.DynamoDBAPI
	TestGroupMeta               *tasks.GroupMeta
	TestDynamoDBScanOutputItems []map[string]*dynamodb.AttributeValue
)

type TestDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI
}

func (t *TestDynamoDBClient) PutItem(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return &dynamodb.PutItemOutput{}, nil
}

func (t *TestDynamoDBClient) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	var output *dynamodb.GetItemOutput
	switch *input.TableName {
	case DBTableGroupMeta:
		output = &dynamodb.GetItemOutput{
			Item: map[string]*dynamodb.AttributeValue{
				"TaskUUIDs": &dynamodb.AttributeValue{
					L: []*dynamodb.AttributeValue{
						&dynamodb.AttributeValue{
							S: aws.String("testTaskUUID1"),
						},
						&dynamodb.AttributeValue{
							S: aws.String("testTaskUUID2"),
						},
						&dynamodb.AttributeValue{
							S: aws.String("testTaskUUID3"),
						},
					},
				},
				"ChordTriggered": &dynamodb.AttributeValue{
					BOOL: aws.Bool(false),
				},
				"GroupUUID": &dynamodb.AttributeValue{
					S: aws.String("testGroupUUID"),
				},
				"Lock": &dynamodb.AttributeValue{
					BOOL: aws.Bool(false),
				},
			},
		}
	case DBTableTaskStates:
		output = &dynamodb.GetItemOutput{
			Item: map[string]*dynamodb.AttributeValue{
				"Error": &dynamodb.AttributeValue{
					NULL: aws.Bool(false),
				},
				"State": &dynamodb.AttributeValue{
					S: aws.String(tasks.StatePending),
				},
				"TaskUUID": &dynamodb.AttributeValue{
					S: aws.String("testTaskUUID1"),
				},
				"Results:": &dynamodb.AttributeValue{
					NULL: aws.Bool(true),
				},
			},
		}

	}
	return output, nil
}

func (t *TestDynamoDBClient) DeleteItem(*dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	return &dynamodb.DeleteItemOutput{}, nil
}

func (t *TestDynamoDBClient) Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	return &dynamodb.ScanOutput{Items: TestDynamoDBScanOutputItems}, nil
}

func (t *TestDynamoDBClient) UpdateItem(*dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}

// Always returns error
type TestErrDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI
}

func (t *TestErrDynamoDBClient) PutItem(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return nil, errors.New("error when putting an item")
}

func (t *TestErrDynamoDBClient) GetItem(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return nil, errors.New("error when getting an item")
}

func (t *TestErrDynamoDBClient) DeleteItem(*dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	return nil, errors.New("error when deleting an item")
}

func (t *TestErrDynamoDBClient) Scan(*dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	return nil, errors.New("error when scanning an item")
}

func (t *TestErrDynamoDBClient) UpdateItem(*dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	return nil, errors.New("error when updating an item")
}

func init() {
	TestCnf = &config.Config{
		ResultBackend:   os.Getenv("DYNAMODB_URL"),
		ResultsExpireIn: 30,
	}
	TestSession := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	TestDBClient = new(TestDynamoDBClient)
	TestDynamoDBBackend = &DynamoDBBackend{cnf: TestCnf, client: TestDBClient, session: TestSession}

	TestErrDBClient = new(TestErrDynamoDBClient)
	TestErrDynamoDBBackend = &DynamoDBBackend{cnf: TestCnf, client: TestErrDBClient, session: TestSession}

	TestGroupMeta = &tasks.GroupMeta{
		GroupUUID: "testGroupUUID",
		TaskUUIDs: []string{"testTaskUUID1", "testTaskUUID2", "testTaskUUID3"},
	}
}

func (b *DynamoDBBackend) GetConfig() *config.Config {
	return b.cnf
}

func (b *DynamoDBBackend) GetClient() dynamodbiface.DynamoDBAPI {
	return b.client
}

func (b *DynamoDBBackend) GetSession() *session.Session {
	return b.session
}

func (b *DynamoDBBackend) GetGroupMetaForTest(groupUUID string) (*tasks.GroupMeta, error) {
	return b.getGroupMeta(groupUUID)
}

func (b *DynamoDBBackend) UnmarshalGroupMetaGetItemResultForTest(result *dynamodb.GetItemOutput) (*tasks.GroupMeta, error) {
	return b.unmarshalGroupMetaGetItemResult(result)
}

func (b *DynamoDBBackend) UnmarshalTaskStateGetItemResultForTest(result *dynamodb.GetItemOutput) (*tasks.TaskState, error) {
	return b.unmarshalTaskStateGetItemResult(result)
}

func (b *DynamoDBBackend) SetTaskStateForTest(taskState *tasks.TaskState) error {
	return b.setTaskState(taskState)
}

func (b *DynamoDBBackend) ChordTriggeredForTest(groupUUID string) error {
	return b.chordTriggered(groupUUID)
}

func (b *DynamoDBBackend) UpdateGroupMetaLockForTest(groupUUID string, status bool) error {
	return b.updateGroupMetaLock(groupUUID, status)
}

func (b *DynamoDBBackend) UnlockGroupMetaForTest(groupUUID string) error {
	return b.unlockGroupMeta(groupUUID)
}

func (b *DynamoDBBackend) LockGroupMetaForTest(groupUUID string) error {
	return b.lockGroupMeta(groupUUID)
}

func (b *DynamoDBBackend) GetStatesForTest(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	return b.getStates(taskUUIDs...)
}

func (b *DynamoDBBackend) UpdateToFailureStateWithErrorForTest(taskState *tasks.TaskState) error {
	return b.updateToFailureStateWithError(taskState)
}
