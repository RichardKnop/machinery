package dynamodb

import (
	"errors"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestTriggerChordPollingReadError(t *testing.T) {
	for _, scenario := range []string{"locked then read error", "initial read error", "already triggered"} {
		t.Run(scenario, func(t *testing.T) {
			var calls int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := atomic.AddInt32(&calls, 1)
				if r.Header.Get("X-Amz-Target") != "DynamoDB_20120810.GetItem" {
					t.Errorf("unexpected operation %s", r.Header.Get("X-Amz-Target"))
				}
				w.Header().Set("Content-Type", "application/x-amz-json-1.0")
				if scenario == "initial read error" || n > 1 {
					w.WriteHeader(http.StatusBadRequest)
					w.Write([]byte(`{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"fixture read failure"}`))
					return
				}
				if scenario == "already triggered" {
					w.Write([]byte(`{"Item":{"GroupUUID":{"S":"group"},"Lock":{"BOOL":false},"ChordTriggered":{"BOOL":true}}}`))
					return
				}
				w.Write([]byte(`{"Item":{"GroupUUID":{"S":"group"},"Lock":{"BOOL":true},"ChordTriggered":{"BOOL":false}}}`))
			}))
			defer server.Close()
			sdkClient := sdk.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("fixture", "fixture", ""), RetryMaxAttempts: 1, HTTPClient: server.Client()}, func(o *sdk.Options) { o.BaseEndpoint = aws.String(server.URL) })
			b := &Backend{cnf: &config.Config{DynamoDB: &config.DynamoDBConfig{GroupMetasTable: "groups"}}, client: sdkClient}
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("TriggerChord panicked after a read error: %v", p)
				}
			}()
			triggered, err := b.TriggerChord("group")
			if scenario == "already triggered" {
				if triggered || err != nil {
					t.Fatalf("triggered=%v error=%v", triggered, err)
				}
				return
			}
			var sdkErr *sdktypes.ResourceNotFoundException
			ok := errors.As(err, &sdkErr)
			if triggered || !ok || sdkErr.ErrorCode() != "ResourceNotFoundException" {
				t.Fatalf("triggered=%v error=%v; want original resource-not-found error", triggered, err)
			}
			want := int32(1)
			if scenario == "locked then read error" {
				want = 2
			}
			if got := atomic.LoadInt32(&calls); got != want {
				t.Fatalf("read calls=%d; want %d", got, want)
			}
		})
	}
}
