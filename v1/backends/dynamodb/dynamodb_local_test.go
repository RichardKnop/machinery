package dynamodb_test

import (
	"net/url"
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v1/backends/dynamodb"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awsdynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestDynamoDBLocalPurgeStringKeys(t *testing.T) {
	endpoint := os.Getenv("MACHINERY_DYNAMODB_LOCAL_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("requires a fresh task-exclusive DynamoDB Local instance")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "http" || parsed.Hostname() != "127.0.0.1" {
		t.Fatal("test endpoint must be an explicit loopback DynamoDB Local instance")
	}
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"), Endpoint: aws.String(endpoint),
		Credentials: credentials.NewStaticCredentials("DUMMYIDEXAMPLE", "DUMMYEXAMPLEKEY", ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	client := awsdynamodb.New(sess)
	tables, err := client.ListTables(&awsdynamodb.ListTablesInput{})
	if err != nil {
		t.Fatal(err)
	}
	if len(tables.TableNames) != 0 {
		t.Fatal("test requires a fresh empty DynamoDB Local instance")
	}
	for table, key := range map[string]string{"task_states": "TaskUUID", "group_metas": "GroupUUID"} {
		_, err := client.CreateTable(&awsdynamodb.CreateTableInput{
			TableName:             aws.String(table),
			AttributeDefinitions:  []*awsdynamodb.AttributeDefinition{{AttributeName: aws.String(key), AttributeType: aws.String("S")}},
			KeySchema:             []*awsdynamodb.KeySchemaElement{{AttributeName: aws.String(key), KeyType: aws.String("HASH")}},
			ProvisionedThroughput: &awsdynamodb.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(1), WriteCapacityUnits: aws.Int64(1)},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer client.DeleteTable(&awsdynamodb.DeleteTableInput{TableName: aws.String(table)})
	}
	backend := dynamodb.New(&config.Config{DynamoDB: &config.DynamoDBConfig{
		TaskStatesTable: "task_states", GroupMetasTable: "group_metas", Client: client,
	}})
	for _, tc := range []struct {
		name, table, key, id string
		seed                 func() error
		purge                func() error
	}{
		{"task state", "task_states", "TaskUUID", "task-uuid", func() error {
			return backend.SetStatePending(&tasks.Signature{UUID: "task-uuid", Name: "synthetic-task"})
		}, func() error { return backend.PurgeState("task-uuid") }},
		{"group metadata", "group_metas", "GroupUUID", "group-uuid", func() error {
			return backend.InitGroup("group-uuid", []string{"task-uuid"})
		}, func() error { return backend.PurgeGroupMeta("group-uuid") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.seed(); err != nil {
				t.Fatal(err)
			}
			input := &awsdynamodb.GetItemInput{TableName: aws.String(tc.table), ConsistentRead: aws.Bool(true),
				Key: map[string]*awsdynamodb.AttributeValue{tc.key: {S: aws.String(tc.id)}}}
			before, err := client.GetItem(input)
			if err != nil || len(before.Item) == 0 {
				t.Fatalf("seeded item not readable: %v", err)
			}
			if err := tc.purge(); err != nil {
				t.Fatal(err)
			}
			after, err := client.GetItem(input)
			if err != nil || len(after.Item) != 0 {
				t.Fatalf("purged item remains or read failed: %v", err)
			}
		})
	}
}
