package sqs_test

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/RichardKnop/machinery/v2/brokers/sqs"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
)

func TestPublishSerializesResolvedRoutingKey(t *testing.T) {
	for _, tc := range []struct{ name, defaultQueue, routingKey, want string }{
		{"default", "default-queue", "", "default-queue"},
		{"explicit", "default-queue", "custom-queue", "custom-queue"},
		{"default FIFO", "default.fifo", "", "default.fifo"},
		{"explicit FIFO", "default-queue", "custom.fifo", "custom.fifo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type request struct{ QueueUrl, MessageBody, MessageGroupId, MessageDeduplicationId string }
			received := make(chan request, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var input request
				jsonProtocol := strings.Contains(r.Header.Get("Content-Type"), "json")
				if jsonProtocol {
					if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
						t.Error(err)
						http.Error(w, "invalid JSON", 400)
						return
					}
				} else {
					if err := r.ParseForm(); err != nil {
						t.Error(err)
						http.Error(w, "invalid form", 400)
						return
					}
					input = request{r.Form.Get("QueueUrl"), r.Form.Get("MessageBody"), r.Form.Get("MessageGroupId"), r.Form.Get("MessageDeduplicationId")}
				}
				received <- input
				digest := fmt.Sprintf("%x", md5.Sum([]byte(input.MessageBody)))
				if jsonProtocol {
					w.Header().Set("Content-Type", "application/x-amz-json-1.0")
					fmt.Fprintf(w, `{"MessageId":"synthetic-message","MD5OfMessageBody":"%s"}`, digest)
				} else {
					w.Header().Set("Content-Type", "text/xml")
					fmt.Fprintf(w, `<SendMessageResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/"><SendMessageResult><MessageId>synthetic-message</MessageId><MD5OfMessageBody>%s</MD5OfMessageBody></SendMessageResult><ResponseMetadata><RequestId>synthetic-request</RequestId></ResponseMetadata></SendMessageResponse>`, digest)
				}
			}))
			defer server.Close()
			client := awssqs.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, HTTPClient: server.Client()}, func(o *awssqs.Options) { o.BaseEndpoint = aws.String(server.URL) })
			broker, err := sqs.New(&config.Config{Broker: server.URL + "/123456789012", DefaultQueue: tc.defaultQueue, SQS: &config.SQSConfig{Client: client}})
			if err != nil {
				t.Fatal(err)
			}
			signature := &tasks.Signature{UUID: "task-uuid", Name: "synthetic-task", RoutingKey: tc.routingKey, BrokerMessageGroupId: "group"}
			if err := broker.Publish(context.Background(), signature); err != nil {
				t.Fatal(err)
			}
			input := <-received
			var decoded tasks.Signature
			if err := json.Unmarshal([]byte(input.MessageBody), &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded.RoutingKey != tc.want || input.QueueUrl != server.URL+"/123456789012/"+tc.want {
				t.Errorf("payload routing=%q, queue=%q; want %q", decoded.RoutingKey, input.QueueUrl, tc.want)
			}
			if decoded.UUID != signature.UUID || decoded.Name != signature.Name {
				t.Fatal("task identity changed")
			}
			if strings.HasSuffix(tc.want, ".fifo") && (input.MessageGroupId != "group" || input.MessageDeduplicationId != signature.UUID) {
				t.Errorf("FIFO identifiers = %q, %q", input.MessageGroupId, input.MessageDeduplicationId)
			}
		})
	}
}
