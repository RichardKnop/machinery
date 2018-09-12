package dynamodb

import (
	"errors"
	"os"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

var (
	TestDynamoDBBackend    *Backend
	TestErrDynamoDBBackend *Backend
	TestCnf                *config.Config
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

func (t *TestDynamoDBClient) ListTables(*dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
	return &dynamodb.ListTablesOutput{
		TableNames: []*string{
			aws.String("group_metas"),
			aws.String("task_states"),
		},
	}, nil
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

func (t *TestErrDynamoDBClient) ListTables(*dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
	return nil, errors.New("error when listing tables")
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
	TestDBClient = new(TestDynamoDBClient)
	TestDynamoDBBackend = &Backend{cnf: TestCnf, client: TestDBClient}

	TestErrDBClient = new(TestErrDynamoDBClient)
	TestErrDynamoDBBackend = &Backend{cnf: TestCnf, client: TestErrDBClient}

	TestGroupMeta = &tasks.GroupMeta{
		GroupUUID: "testGroupUUID",
		TaskUUIDs: []string{"testTaskUUID1", "testTaskUUID2", "testTaskUUID3"},
	}
}

func (b *Backend) GetConfig() *config.Config {
	return b.cnf
}

func (b *Backend) GetClient() dynamodbiface.DynamoDBAPI {
	return b.client
}

func (b *Backend) GetGroupMetaForTest(groupUUID string) (*tasks.GroupMeta, error) {
	return b.getGroupMeta(groupUUID)
}

func (b *Backend) UnmarshalGroupMetaGetItemResultForTest(result *dynamodb.GetItemOutput) (*tasks.GroupMeta, error) {
	return b.unmarshalGroupMetaGetItemResult(result)
}

func (b *Backend) UnmarshalTaskStateGetItemResultForTest(result *dynamodb.GetItemOutput) (*tasks.TaskState, error) {
	return b.unmarshalTaskStateGetItemResult(result)
}

func (b *Backend) SetTaskStateForTest(taskState *tasks.TaskState) error {
	return b.setTaskState(taskState)
}

func (b *Backend) ChordTriggeredForTest(groupUUID string) error {
	return b.chordTriggered(groupUUID)
}

func (b *Backend) UpdateGroupMetaLockForTest(groupUUID string, status bool) error {
	return b.updateGroupMetaLock(groupUUID, status)
}

func (b *Backend) UnlockGroupMetaForTest(groupUUID string) error {
	return b.unlockGroupMeta(groupUUID)
}

func (b *Backend) LockGroupMetaForTest(groupUUID string) error {
	return b.lockGroupMeta(groupUUID)
}

func (b *Backend) GetStatesForTest(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	return b.getStates(taskUUIDs...)
}

func (b *Backend) UpdateToFailureStateWithErrorForTest(taskState *tasks.TaskState) error {
	return b.updateToFailureStateWithError(taskState)
}

func (b *Backend) TableExistsForTest(tableName string, tableNames []*string) bool {
	return b.tableExists(tableName, tableNames)
}

func (b *Backend) CheckRequiredTablesIfExistForTest() error {
	return b.checkRequiredTablesIfExist()
}
