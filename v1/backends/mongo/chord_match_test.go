package mongo

import (
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	drivermongo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"testing"
)

func TestTriggerChordMatchedUpdate(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	mt.Run("first then already triggered", func(mt *mtest.T) {
		b := &Backend{gmc: mt.Coll}
		b.once.Do(func() {})
		mt.AddMockResponses(mtest.CreateSuccessResponse(bson.E{Key: "n", Value: 1}, bson.E{Key: "nModified", Value: 1}), mtest.CreateSuccessResponse(bson.E{Key: "n", Value: 0}, bson.E{Key: "nModified", Value: 0}))
		for _, want := range []bool{true, false} {
			triggered, err := b.TriggerChord("group")
			if err != nil || triggered != want {
				mt.Errorf("triggered=%v error=%v; want %v,nil", triggered, err, want)
			}
			event := mt.GetStartedEvent()
			if event == nil || event.CommandName != "update" {
				mt.Fatal("expected driver update command")
			}
			update := event.Command.Lookup("updates").Array().Index(0).Value().Document()
			filter := update.Lookup("q").Document()
			if filter.Lookup("_id").StringValue() != "group" || filter.Lookup("chord_triggered").Boolean() != false {
				mt.Fatal("update lost untriggered-group predicate")
			}
			if !update.Lookup("u").Document().Lookup("$set").Document().Lookup("chord_triggered").Boolean() {
				mt.Fatal("update did not mark chord triggered")
			}
			if upsert, ok := update.Lookup("upsert").BooleanOK(); ok && upsert {
				mt.Fatal("chord trigger must not upsert missing group")
			}
		}
	})
	mt.Run("update error", func(mt *mtest.T) {
		b := &Backend{gmc: mt.Coll}
		b.once.Do(func() {})
		mt.AddMockResponses(mtest.CreateCommandErrorResponse(mtest.CommandError{Code: 2, Message: "update failed"}))
		triggered, err := b.TriggerChord("group")
		var commandError drivermongo.CommandError
		if triggered || !errors.As(err, &commandError) || commandError.Code != 2 {
			mt.Fatalf("triggered=%v error=%v; want false,command error 2", triggered, err)
		}
	})
}
