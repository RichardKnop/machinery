package mongo_test

import (
	"context"
	"fmt"
	backendmongo "github.com/RichardKnop/machinery/v1/backends/mongo"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"testing"
	"time"
)

// The opt-in native fixture uses ttlMonitorSleepSecs=1; this is not a production cleanup deadline.
func TestNativeTaskResultTTL(t *testing.T) {
	uri := os.Getenv("MACHINERY_MONGO_TTL_URI")
	if uri == "" {
		t.Skip("native MongoDB TTL fixture not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(context.Background())
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatal(err)
	}
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy-indexes=%v", legacy), func(t *testing.T) {
			name := fmt.Sprintf("native_ttl_%v", legacy)
			db := client.Database(name)
			db.Drop(ctx)
			defer db.Drop(context.Background())
			collection := db.Collection("tasks")
			if legacy {
				_, err := collection.Indexes().CreateMany(ctx, []mongo.IndexModel{{Keys: bson.D{{Key: "state", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(1)}, {Keys: bson.D{{Key: "lock", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(1)}})
				if err != nil {
					t.Fatal(err)
				}
			}
			backend, err := backendmongo.New(&config.Config{ResultsExpireIn: 1, MongoDB: &config.MongoDBConfig{Client: client, Database: name}})
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"success", "failure", "pending"} {
				if err := backend.SetStatePending(&tasks.Signature{UUID: id, Name: "task"}); err != nil {
					t.Fatal(err)
				}
			}
			if err := backend.SetStateSuccess(&tasks.Signature{UUID: "success"}, nil); err != nil {
				t.Fatal(err)
			}
			if err := backend.SetStateFailure(&tasks.Signature{UUID: "failure"}, "failed"); err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"success", "failure"} {
				if state, err := backend.GetState(id); err != nil || state.TaskUUID != id {
					t.Fatalf("completed result decode: state=%v error=%v", state, err)
				}
			}
			deadline := time.Now().Add(8 * time.Second)
			var count int64
			for {
				count, err = collection.CountDocuments(ctx, bson.M{"_id": bson.M{"$in": []string{"success", "failure"}}})
				if err != nil {
					t.Fatal(err)
				}
				if count == 0 || time.Now().After(deadline) {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if count != 0 {
				t.Errorf("completed results remaining=%d; want TTL deletion", count)
			}
			if pending, err := backend.GetState("pending"); err != nil || pending.State != tasks.StatePending {
				t.Fatalf("pending task expired: state=%v error=%v", pending, err)
			}
			indexes, err := collection.Indexes().List(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer indexes.Close(ctx)
			found := map[string]bool{}
			for indexes.Next(ctx) {
				var index bson.M
				if err := indexes.Decode(&index); err != nil {
					t.Fatal(err)
				}
				found[index["name"].(string)] = true
			}
			if err := indexes.Err(); err != nil {
				t.Fatal(err)
			}
			if !found["delete_at_1"] {
				t.Error("date TTL index missing")
			}
			if legacy && (!found["state_1"] || !found["lock_1"]) {
				t.Error("existing indexes were removed")
			}
		})
	}
	t.Run("default retention", func(t *testing.T) {
		db := client.Database("native_ttl_default")
		db.Drop(ctx)
		defer db.Drop(context.Background())
		backend, err := backendmongo.New(&config.Config{MongoDB: &config.MongoDBConfig{Client: client, Database: db.Name()}})
		if err != nil {
			t.Fatal(err)
		}
		before := time.Now().Add(time.Duration(config.DefaultResultsExpireIn) * time.Second).Truncate(time.Millisecond)
		if err := backend.SetStateSuccess(&tasks.Signature{UUID: "default"}, nil); err != nil {
			t.Fatal(err)
		}
		after := time.Now().Add(time.Duration(config.DefaultResultsExpireIn) * time.Second).Truncate(time.Millisecond)
		var doc bson.Raw
		if err := db.Collection("tasks").FindOne(ctx, bson.M{"_id": "default"}).Decode(&doc); err != nil {
			t.Fatal(err)
		}
		value, ok := doc.Lookup("delete_at").DateTimeOK()
		if !ok {
			t.Fatal("default result has no date expiry")
		}
		expiry := time.UnixMilli(value)
		if expiry.Before(before) || expiry.After(after) {
			t.Fatalf("expiry=%v; want [%v,%v]", expiry, before, after)
		}
	})
}
