package backends

import (
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"os"
)

var (
	TestDynamoDBBackend *DynamoDBBackend
	TestCnf             *config.Config
	TestSession         *session.Session
	TestDBClient        dynamodbiface.DynamoDBAPI
)

type TestDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI
}

func (t *TestDynamoDBClient) PutItem(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return &dynamodb.PutItemOutput{}, nil
}

func (t *TestDynamoDBClient) GetItem(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{}, nil
}

func (t *TestDynamoDBClient) DeleteItem(*dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	return &dynamodb.DeleteItemOutput{}, nil
}

func (t *TestDynamoDBClient) Scan(*dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	return &dynamodb.ScanOutput{}, nil
}

func (t *TestDynamoDBClient) UpdateItem(*dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}

func init() {
	TestCnf = &config.Config{
		ResultBackend:   os.Getenv("DYNAMODB_URL"),
		ResultsExpireIn: 30,
	}
	TestSession := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	TestDBClient := new(TestDynamoDBClient)
	TestDynamoDBBackend = &DynamoDBBackend{cnf: TestCnf, client: TestDBClient, session: TestSession}
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
