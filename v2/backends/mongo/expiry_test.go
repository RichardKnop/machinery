package mongo

import (
	"fmt"
	"github.com/RichardKnop/machinery/v2/common"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/tasks"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"testing"
	"time"
)

func TestCompletedStateExpiration(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	for _, expiry := range []int{0, 42, -1} {
		for _, state := range []string{tasks.StateSuccess, tasks.StateFailure} {
			mt.Run(fmt.Sprintf("%s/%d", state, expiry), func(mt *mtest.T) {
				b := &Backend{Backend: common.NewBackend(&config.Config{ResultsExpireIn: expiry}), tc: mt.Coll}
				b.once.Do(func() {})
				mt.AddMockResponses(mtest.CreateSuccessResponse(bson.E{Key: "n", Value: 1}, bson.E{Key: "nModified", Value: 1}))
				before := time.Now().Truncate(time.Millisecond)
				var err error
				if state == tasks.StateSuccess {
					err = b.SetStateSuccess(&tasks.Signature{UUID: "task"}, nil)
				} else {
					err = b.SetStateFailure(&tasks.Signature{UUID: "task"}, "task failed")
				}
				after := time.Now().Truncate(time.Millisecond)
				if err != nil {
					mt.Fatal(err)
				}
				event := mt.GetStartedEvent()
				if event == nil || event.CommandName != "update" {
					mt.Fatal("missing update command")
				}
				update := event.Command.Lookup("updates").Array().Index(0).Value().Document()
				if update.Lookup("q").Document().Lookup("_id").StringValue() != "task" {
					mt.Fatal("wrong task filter")
				}
				fields := update.Lookup("u").Document().Lookup("$set").Document()
				if fields.Lookup("state").StringValue() != state {
					mt.Fatal("wrong task state")
				}
				expiryTime := fields.Lookup("delete_at").DateTime()
				got := time.UnixMilli(expiryTime)
				seconds := expiry
				if seconds == 0 {
					seconds = config.DefaultResultsExpireIn
				}
				duration := time.Duration(seconds) * time.Second
				if got.Before(before.Add(duration)) || got.After(after.Add(duration)) {
					mt.Fatalf("delete_at=%v; want between %v and %v", got, before.Add(duration), after.Add(duration))
				}
			})
		}
	}
}
