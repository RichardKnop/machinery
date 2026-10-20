package mongo

import (
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	drivermongo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"testing"
)

func TestGetStatesCursorErrors(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))
	for _, scenario := range []string{"getMore error", "find error", "complete cursor"} {
		mt.Run(scenario, func(mt *mtest.T) {
			b := &Backend{tc: mt.Coll}
			b.once.Do(func() {}) // Use the driver's mock deployment, not a real database.
			ns := mt.DB.Name() + "." + mt.Coll.Name()
			first := bson.D{{Key: "_id", Value: "first"}, {Key: "state", Value: "SUCCESS"}}
			second := bson.D{{Key: "_id", Value: "second"}, {Key: "state", Value: "SUCCESS"}}
			code := int32(43)
			if scenario == "find error" {
				code = 2
				mt.AddMockResponses(mtest.CreateCommandErrorResponse(mtest.CommandError{Code: code, Message: "find failed"}))
			} else {
				mt.AddMockResponses(mtest.CreateCursorResponse(123, ns, mtest.FirstBatch, first))
				if scenario == "getMore error" {
					mt.AddMockResponses(mtest.CreateCommandErrorResponse(mtest.CommandError{Code: code, Message: "cursor lost"}), mtest.CreateSuccessResponse())
				} else {
					mt.AddMockResponses(mtest.CreateCursorResponse(0, ns, mtest.NextBatch, second))
				}
			}
			states, err := b.getStates("first", "second")
			if scenario == "complete cursor" {
				if err != nil || len(states) != 2 || states[0].TaskUUID != "first" || states[1].TaskUUID != "second" {
					mt.Fatalf("states=%v error=%v", states, err)
				}
				return
			}
			var commandError drivermongo.CommandError
			if !errors.As(err, &commandError) || commandError.Code != code {
				mt.Fatalf("error=%v; want driver command error %d", err, code)
			}
			if states != nil {
				mt.Fatalf("error returned partial states: %v", states)
			}
		})
	}
}
