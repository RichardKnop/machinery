package dynamodb

import (
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
)

var (
	TestDynamoDBBackend *Backend
	TestCnf             *config.Config
)

type TestDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI
	PutItemOverride      func(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	UpdateItemOverride   func(*dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)
	BatchGetItemOverride func(*dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error)
}

func (t *TestDynamoDBClient) ResetOverrides() {
	t.PutItemOverride = nil
	t.UpdateItemOverride = nil
	t.BatchGetItemOverride = nil
}

func (t *TestDynamoDBClient) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	if t.PutItemOverride != nil {
		return t.PutItemOverride(input)
	}
	return t.DynamoDBAPI.PutItem(input)
}
func (t *TestDynamoDBClient) BatchGetItem(input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error) {
	if t.BatchGetItemOverride != nil {
		return t.BatchGetItemOverride(input)
	}
	return t.DynamoDBAPI.BatchGetItem(input)
}

func (t *TestDynamoDBClient) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	return t.DynamoDBAPI.DeleteItem(input)
}

func (t *TestDynamoDBClient) UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	if t.UpdateItemOverride != nil {
		return t.UpdateItemOverride(input)
	}
	return t.DynamoDBAPI.UpdateItem(input)
}

func init() {
	os.Setenv("AWS_ACCESS_KEY", "xxx")
	os.Setenv("AWS_SECRET_KEY", "xxx")
	cred := credentials.NewEnvCredentials()

	awsSession, _ := session.NewSession()
	dynamodbClient := dynamodb.New(awsSession, &aws.Config{
		Region:      aws.String("us-west-2"),
		Credentials: cred,
		Endpoint:    aws.String(os.Getenv("DYNAMODB_URL")),
	})

	TestCnf = &config.Config{
		ResultBackend:   os.Getenv("DYNAMODB_URL"),
		ResultsExpireIn: 30,
		DynamoDB: &config.DynamoDBConfig{
			TaskStatesTable: "task_states",
			GroupMetasTable: "group_metas",
			Client:          dynamodbClient,
		},
	}

	TestDynamoDBBackend = &Backend{cnf: TestCnf, client: &TestDynamoDBClient{DynamoDBAPI: dynamodbClient}}

	dynamodbClient.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("TaskUUID"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("TaskUUID"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
		TableName: aws.String("task_states"),
	})

	dynamodbClient.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("GroupUUID"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("GroupUUID"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
		TableName: aws.String("group_metas"),
	})
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
	return b.getStates(taskUUIDs)
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
