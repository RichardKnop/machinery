package dynamodb

import (
	"fmt"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	sdk "github.com/aws/aws-sdk-go/service/dynamodb"
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
	sess, err := session.NewSession(&aws.Config{Endpoint: aws.String(endpoint), Region: aws.String("us-east-1"), Credentials: credentials.NewStaticCredentials("fixture", "fixture", ""), MaxRetries: aws.Int(0)})
	if err != nil {
		t.Fatal(err)
	}
	client := sdk.New(sess)
	for _, table := range []struct{ name, key string }{{"native_chord_v1_states", "TaskUUID"}, {"native_chord_v1_groups", "GroupUUID"}} {
		_, err := client.CreateTable(&sdk.CreateTableInput{TableName: aws.String(table.name), AttributeDefinitions: []*sdk.AttributeDefinition{{AttributeName: aws.String(table.key), AttributeType: aws.String("S")}}, KeySchema: []*sdk.KeySchemaElement{{AttributeName: aws.String(table.key), KeyType: aws.String("HASH")}}, ProvisionedThroughput: &sdk.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(5), WriteCapacityUnits: aws.Int64(5)}})
		if err != nil {
			t.Fatal(err)
		}
	}
	cnf := &config.Config{ResultsExpireIn: 3600, DynamoDB: &config.DynamoDBConfig{Client: client, TaskStatesTable: "native_chord_v1_states", GroupMetasTable: "native_chord_v1_groups"}}
	backend := New(cnf)
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
