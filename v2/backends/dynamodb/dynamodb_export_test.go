package dynamodb

import (
	"context"
	"errors"
	"os"

	dynamodbiface2 "github.com/RichardKnop/machinery/v2/backends/iface/dynamodb"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	TestDynamoDBBackend    *Backend
	TestErrDynamoDBBackend *Backend
	TestCnf                *config.Config
	TestDBClient           dynamodbiface2.API
	TestErrDBClient        dynamodbiface2.API
	TestGroupMeta          *tasks.GroupMeta
	TestTask1              map[string]types.AttributeValue
	TestTask2              map[string]types.AttributeValue
	TestTask3              map[string]types.AttributeValue
)

type TestDynamoDBClient struct {
	dynamodbiface2.API
	PutItemOverride      func(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItemOverride   func(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	GetItemOverride      func(ctx context.Context, input *dynamodb.GetItemInput, ops ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	BatchGetItemOverride func(context.Context, *dynamodb.BatchGetItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
}

func (t *TestDynamoDBClient) ResetOverrides() {
	t.PutItemOverride = nil
	t.UpdateItemOverride = nil
	t.BatchGetItemOverride = nil
}

func (t *TestDynamoDBClient) PutItem(ctx context.Context, input *dynamodb.PutItemInput, ops ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if t.PutItemOverride != nil {
		return t.PutItemOverride(ctx, input, ops...)
	}
	return &dynamodb.PutItemOutput{}, nil
}
func (t *TestDynamoDBClient) BatchGetItem(ctx context.Context, input *dynamodb.BatchGetItemInput, ops ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	if t.BatchGetItemOverride != nil {
		return t.BatchGetItemOverride(ctx, input, ops...)
	}
	return &dynamodb.BatchGetItemOutput{}, nil
}

func (t *TestDynamoDBClient) GetItem(ctx context.Context, input *dynamodb.GetItemInput, ops ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if t.GetItemOverride != nil {
		return t.GetItemOverride(ctx, input, ops...)
	}
	var output *dynamodb.GetItemOutput
	switch *input.TableName {
	case "group_metas":
		output = &dynamodb.GetItemOutput{
			Item: map[string]types.AttributeValue{
				"TaskUUIDs": &types.AttributeValueMemberL{
					Value: []types.AttributeValue{
						&types.AttributeValueMemberS{
							Value: "testTaskUUID1",
						},
						&types.AttributeValueMemberS{
							Value: "testTaskUUID2",
						},
						&types.AttributeValueMemberS{
							Value: "testTaskUUID3",
						},
					},
				},
				"ChordTriggered": &types.AttributeValueMemberBOOL{
					Value: false,
				},
				"GroupUUID": &types.AttributeValueMemberS{
					Value: "testGroupUUID",
				},
				"Lock": &types.AttributeValueMemberBOOL{
					Value: false,
				},
			},
		}
	case "task_states":
		if input.Key["TaskUUID"] == nil {
			output = &dynamodb.GetItemOutput{
				Item: map[string]types.AttributeValue{
					"Error": &types.AttributeValueMemberNULL{
						Value: false,
					},
					"State": &types.AttributeValueMemberS{
						Value: tasks.StatePending,
					},
					"TaskUUID": &types.AttributeValueMemberS{
						Value: "testTaskUUID1",
					},
					"Results:": &types.AttributeValueMemberNULL{
						Value: true,
					},
				},
			}
		} else {
			if input.Key["TaskUUID"].(*types.AttributeValueMemberS).Value == "testTaskUUID1" {
				output = &dynamodb.GetItemOutput{
					Item: TestTask1,
				}
			} else if input.Key["TaskUUID"].(*types.AttributeValueMemberS).Value == "testTaskUUID2" {
				output = &dynamodb.GetItemOutput{
					Item: TestTask2,
				}

			} else if input.Key["TaskUUID"].(*types.AttributeValueMemberS).Value == "testTaskUUID3" {
				output = &dynamodb.GetItemOutput{
					Item: TestTask3,
				}
			}
		}

	}
	return output, nil
}

func (t *TestDynamoDBClient) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, ops ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return &dynamodb.DeleteItemOutput{}, nil
}

func (t *TestDynamoDBClient) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, ops ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if t.UpdateItemOverride != nil {
		return t.UpdateItemOverride(ctx, input, ops...)
	}
	return &dynamodb.UpdateItemOutput{}, nil
}

func (t *TestDynamoDBClient) ListTables(ctx context.Context, input *dynamodb.ListTablesInput, ops ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error) {
	return &dynamodb.ListTablesOutput{
		TableNames: []string{
			"group_metas",
			"task_states",
		},
	}, nil
}

// Always returns error
type TestErrDynamoDBClient struct {
	dynamodbiface2.API
}

func (t *TestErrDynamoDBClient) PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return nil, errors.New("error when putting an item")
}

func (t *TestErrDynamoDBClient) GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return nil, errors.New("error when getting an item")
}

func (t *TestErrDynamoDBClient) DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return nil, errors.New("error when deleting an item")
}

func (t *TestErrDynamoDBClient) Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	return nil, errors.New("error when scanning an item")
}

func (t *TestErrDynamoDBClient) UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return nil, errors.New("error when updating an item")
}

func (t *TestErrDynamoDBClient) ListTables(context.Context, *dynamodb.ListTablesInput, ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error) {
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

func (b *Backend) GetClient() dynamodbiface2.API {
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
	return b.getStates(taskUUIDs)
}

func (b *Backend) UpdateToFailureStateWithErrorForTest(taskState *tasks.TaskState) error {
	return b.updateToFailureStateWithError(taskState)
}

func (b *Backend) TableExistsForTest(tableName string, tableNames []string) bool {
	return b.tableExists(tableName, tableNames)
}

func (b *Backend) CheckRequiredTablesIfExistForTest() error {
	return b.checkRequiredTablesIfExist()
}
