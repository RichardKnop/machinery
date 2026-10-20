package dynamodb_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/RichardKnop/machinery/v2/backends/dynamodb"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func TestGroupTaskStatesSDKPartialResponse(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial=%v", partial), func(t *testing.T) {
			var batchCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/x-amz-json-1.0")
				switch r.Header.Get("X-Amz-Target") {
				case "DynamoDB_20120810.ListTables":
					fmt.Fprint(w, `{"TableNames":["task_states","group_metas"]}`)
				case "DynamoDB_20120810.GetItem":
					fmt.Fprint(w, `{"Item":{"GroupUUID":{"S":"group"},"TaskUUIDs":{"L":[{"S":"task-1"},{"S":"task-2"}]}}}`)
				case "DynamoDB_20120810.BatchGetItem":
					call := batchCalls.Add(1)
					var request struct {
						RequestItems map[string]struct {
							Keys           []map[string]struct{ S string }
							ConsistentRead bool
						}
					}
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
						http.Error(w, "invalid request", http.StatusBadRequest)
						return
					}
					items := request.RequestItems["task_states"]
					ids := make([]string, len(items.Keys))
					for i, key := range items.Keys {
						ids[i] = key["TaskUUID"].S
					}
					want := []string{"task-1", "task-2"}
					if call == 2 {
						want = []string{"task-2"}
					}
					if !reflect.DeepEqual(ids, want) || !items.ConsistentRead {
						t.Errorf("batch request = %v, consistent=%v; want %v, true", ids, items.ConsistentRead, want)
					}
					if partial && call == 1 {
						fmt.Fprint(w, `{"Responses":{"task_states":[{"TaskUUID":{"S":"task-1"},"State":{"S":"SUCCESS"}}]},"UnprocessedKeys":{"task_states":{"Keys":[{"TaskUUID":{"S":"task-2"}}]}}}`)
					} else if partial {
						fmt.Fprint(w, `{"Responses":{"task_states":[{"TaskUUID":{"S":"task-2"},"State":{"S":"SUCCESS"}}]},"UnprocessedKeys":{}}`)
					} else {
						fmt.Fprint(w, `{"Responses":{"task_states":[{"TaskUUID":{"S":"task-1"},"State":{"S":"SUCCESS"}},{"TaskUUID":{"S":"task-2"},"State":{"S":"SUCCESS"}}]},"UnprocessedKeys":{}}`)
					}
				default:
					t.Errorf("unexpected operation %q", r.Header.Get("X-Amz-Target"))
					http.Error(w, "unexpected operation", http.StatusBadRequest)
				}
			}))
			defer server.Close()
			client := awsdynamodb.NewFromConfig(aws.Config{
				Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, HTTPClient: server.Client(),
			}, func(o *awsdynamodb.Options) { o.BaseEndpoint = aws.String(server.URL) })
			backend, err := dynamodb.New(&config.Config{DynamoDB: &config.DynamoDBConfig{
				TaskStatesTable: "task_states", GroupMetasTable: "group_metas", Client: client,
			}})
			if err != nil {
				t.Fatal(err)
			}
			states, err := backend.GroupTaskStates("group", 2)
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, len(states))
			for i, state := range states {
				ids[i] = state.TaskUUID
			}
			sort.Strings(ids)
			if !reflect.DeepEqual(ids, []string{"task-1", "task-2"}) {
				t.Errorf("task states = %v; want both requested tasks", ids)
			}
			wantCalls := int32(1)
			if partial {
				wantCalls = 2
			}
			if batchCalls.Load() != wantCalls {
				t.Errorf("BatchGetItem calls = %d; want %d", batchCalls.Load(), wantCalls)
			}
		})
	}
}
