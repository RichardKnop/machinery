package dynamodb

import (
	"context"
	"fmt"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"os"
	"sync"
	"testing"
	"time"
)

func TestNativeChordAtomicClaim(t *testing.T) {
	endpoint := os.Getenv("MACHINERY_DYNAMO_CHORD_ENDPOINT")
	if endpoint == "" {
		t.Skip("native DynamoDB fixture not configured")
	}
	client := sdk.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("fixture", "fixture", ""), RetryMaxAttempts: 1}, func(o *sdk.Options) { o.BaseEndpoint = aws.String(endpoint) })
	for _, table := range []struct{ name, key string }{{"native_chord_v2_states", "TaskUUID"}, {"native_chord_v2_groups", "GroupUUID"}} {
		_, err := client.CreateTable(context.Background(), &sdk.CreateTableInput{TableName: aws.String(table.name), AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String(table.key), AttributeType: types.ScalarAttributeTypeS}}, KeySchema: []types.KeySchemaElement{{AttributeName: aws.String(table.key), KeyType: types.KeyTypeHash}}, ProvisionedThroughput: &types.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(5), WriteCapacityUnits: aws.Int64(5)}})
		if err != nil {
			t.Fatal(err)
		}
	}
	cnf := &config.Config{ResultsExpireIn: 3600, DynamoDB: &config.DynamoDBConfig{Client: client, TaskStatesTable: "native_chord_v2_states", GroupMetasTable: "native_chord_v2_groups"}}
	backend, err := New(cnf)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 16
	for round := 0; round < 10; round++ {
		id := fmt.Sprintf("chord-%d", round)
		if err := backend.InitGroup(id, []string{"task"}); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		outcomes := make(chan bool, workers)
		failures := make(chan error, workers)
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if p := recover(); p != nil {
						outcomes <- false
						failures <- fmt.Errorf("panic: %v", p)
					}
				}()
				<-start
				triggered, err := backend.TriggerChord(id)
				outcomes <- triggered
				failures <- err
			}()
		}
		close(start)
		wg.Wait()
		close(outcomes)
		close(failures)
		winners := 0
		for triggered := range outcomes {
			if triggered {
				winners++
			}
		}
		for err := range failures {
			if err != nil {
				t.Error(err)
			}
		}
		if winners != 1 {
			t.Errorf("round %d: %d claimants; want 1", round, winners)
		}
		if triggered, err := backend.TriggerChord(id); triggered || err != nil {
			t.Errorf("repeat triggered=%v error=%v; want false,nil", triggered, err)
		}
	}
	native := backend.(*Backend)
	if triggered, err := backend.TriggerChord("missing-group"); triggered || err != nil {
		t.Fatalf("missing group: triggered=%v error=%v", triggered, err)
	}
	missing, err := native.getGroupMeta("missing-group")
	if err != nil || missing.GroupUUID != "" {
		t.Fatalf("missing group was created: meta=%+v error=%v", missing, err)
	}
	if err := backend.InitGroup("legacy-lock", []string{"task"}); err != nil {
		t.Fatal(err)
	}
	if err := native.lockGroupMeta("legacy-lock"); err != nil {
		t.Fatal(err)
	}
	defer native.unlockGroupMeta("legacy-lock")
	type claimResult struct {
		triggered bool
		err       error
	}
	done := make(chan claimResult, 1)
	go func() { triggered, err := backend.TriggerChord("legacy-lock"); done <- claimResult{triggered, err} }()
	select {
	case result := <-done:
		t.Fatalf("triggered while legacy lock held: %+v", result)
	case <-time.After(20 * time.Millisecond):
	}
	if err := native.unlockGroupMeta("legacy-lock"); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if !result.triggered || result.err != nil {
			t.Fatalf("after unlock: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("trigger did not resume after unlock")
	}
}
