package backends

import (
	"errors"
	"os"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

var (
	TestDynamoDBBackend    *DynamoDBBackend
	TestErrDynamoDBBackend *DynamoDBBackend
	TestCnf                *config.Config
	TestSession            *session.Session
	TestDBClient           dynamodbiface.DynamoDBAPI
	TestErrDBClient        dynamodbiface.DynamoDBAPI
	TestGroupMeta          *tasks.GroupMeta
	TestTask1              map[string]*dynamodb.AttributeValue
	TestTask2              map[string]*dynamodb.AttributeValue
	TestTask3              map[string]*dynamodb.AttributeValue
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
	case "group_metas":
		output = &dynamodb.GetItemOutput{
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
	case "task_states":
		if input.Key["TaskUUID"] == nil {
			output = &dynamodb.GetItemOutput{
				Item: map[string]*dynamodb.AttributeValue{
					"Error": {
						NULL: aws.Bool(false),
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
		} else {
			if *(input.Key["TaskUUID"].S) == "testTaskUUID1" {
				output = &dynamodb.GetItemOutput{
					Item: TestTask1,
				}
			} else if *(input.Key["TaskUUID"].S) == "testTaskUUID2" {
				output = &dynamodb.GetItemOutput{
					Item: TestTask2,
				}

			} else if *(input.Key["TaskUUID"].S) == "testTaskUUID3" {
				output = &dynamodb.GetItemOutput{
					Item: TestTask3,
				}
			}
		}

	}
	return output, nil
}

func (t *TestDynamoDBClient) DeleteItem(*dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	return &dynamodb.DeleteItemOutput{}, nil
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
		DynamoDB: &config.DynamoDBConfig{
			TaskStatesTable: "task_states",
			GroupMetasTable: "group_metas",
		},
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
