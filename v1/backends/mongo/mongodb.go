package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// Backend represents a MongoDB result backend
type Backend struct {
	common.Backend
	client *mongo.Client
	tc     *mongo.Collection
	gmc    *mongo.Collection
	once   sync.Once
}

// New creates Backend instance
func New(cnf *config.Config) (iface.Backend, error) {
	backend := &Backend{
		Backend: common.NewBackend(cnf),
		once: sync.Once{},
	}

	return backend, nil
}

// InitGroup creates and saves a group meta data object
func (b *Backend) InitGroup(groupUUID string, taskUUIDs []string) error {
	groupMeta := &tasks.GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
		CreatedAt: time.Now().UTC(),
	}
	_, err := b.groupMetasCollection().InsertOne(context.Background(), groupMeta)
	return err
}

// GroupCompleted returns true if all tasks in a group finished
func (b *Backend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}

	var countSuccessTasks = 0
	for _, taskState := range taskStates {
		if taskState.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *Backend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return []*tasks.TaskState{}, err
	}

	return b.getStates(groupMeta.TaskUUIDs...)
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never triggered multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *Backend) TriggerChord(groupUUID string) (bool, error) {
	query := bson.M{
		"_id":             groupUUID,
		"chord_triggered": false,
	}
	change := bson.M{
		"$set": bson.M{
			"chord_triggered": true,
		},
	}

	_, err := b.groupMetasCollection().UpdateOne(context.Background(), query, change, options.Update())

	if err != nil {
		if err == mongo.ErrNoDocuments {
			log.WARNING.Printf("Chord already triggered for group %s", groupUUID)
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// SetStatePending updates task state to PENDING
func (b *Backend) SetStatePending(signature *tasks.Signature) error {
	update := bson.M{
		"state":      tasks.StatePending,
		"task_name":  signature.Name,
		"created_at": time.Now().UTC(),
	}
	return b.updateState(signature, update)
}

// SetStateReceived updates task state to RECEIVED
func (b *Backend) SetStateReceived(signature *tasks.Signature) error {
	update := bson.M{"state": tasks.StateReceived}
	return b.updateState(signature, update)
}

// SetStateStarted updates task state to STARTED
func (b *Backend) SetStateStarted(signature *tasks.Signature) error {
	update := bson.M{"state": tasks.StateStarted}
	return b.updateState(signature, update)
}

// SetStateRetry updates task state to RETRY
func (b *Backend) SetStateRetry(signature *tasks.Signature) error {
	update := bson.M{"state": tasks.StateRetry}
	return b.updateState(signature, update)
}

// SetStateSuccess updates task state to SUCCESS
func (b *Backend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	decodedResults := b.decodeResults(results)
	update := bson.M{
		"state":   tasks.StateSuccess,
		"results": decodedResults,
	}
	return b.updateState(signature, update)
}

// decodeResults detects & decodes json strings in TaskResult.Value and returns a new slice
func (b *Backend) decodeResults(results []*tasks.TaskResult) []*tasks.TaskResult {
	l := len(results)
	jsonResults := make([]*tasks.TaskResult, l, l)
	for i, result := range results {
		jsonResult := new(bson.M)
		resultType := reflect.TypeOf(result.Value).Kind()
		if resultType == reflect.String {
			err := json.NewDecoder(strings.NewReader(result.Value.(string))).Decode(&jsonResult)
			if err == nil {
				jsonResults[i] = &tasks.TaskResult{
					Type:  "json",
					Value: jsonResult,
				}
				continue
			}
		}
		jsonResults[i] = result
	}
	return jsonResults
}

// SetStateFailure updates task state to FAILURE
func (b *Backend) SetStateFailure(signature *tasks.Signature, err string) error {
	update := bson.M{"state": tasks.StateFailure, "error": err}
	return b.updateState(signature, update)
}

// GetState returns the latest task state
func (b *Backend) GetState(taskUUID string) (*tasks.TaskState, error) {
	state := &tasks.TaskState{}
	err := b.tasksCollection().FindOne(context.Background(), bson.M{"_id": taskUUID}).Decode(state)

	if err != nil {
		return nil, err
	}
	return state, nil
}

// PurgeState deletes stored task state
func (b *Backend) PurgeState(taskUUID string) error {
	_, err := b.tasksCollection().DeleteOne(context.Background(), bson.M{"_id": taskUUID})
	return err
}

// PurgeGroupMeta deletes stored group meta data
func (b *Backend) PurgeGroupMeta(groupUUID string) error {
	_, err := b.groupMetasCollection().DeleteOne(context.Background(), bson.M{"_id": groupUUID})
	return err
}

// lockGroupMeta acquires lock on groupUUID document
func (b *Backend) lockGroupMeta(groupUUID string) error {
	query := bson.M{
		"_id":  groupUUID,
		"lock": false,
	}
	change := bson.M{
		"$set": bson.M{
			"lock": true,
		},
	}

	_, err := b.groupMetasCollection().UpdateOne(context.Background(), query, change, options.Update().SetUpsert(true))

	return err
}

// unlockGroupMeta releases lock on groupUUID document
func (b *Backend) unlockGroupMeta(groupUUID string) error {
	update := bson.M{"$set": bson.M{"lock": false}}
	_, err := b.groupMetasCollection().UpdateOne(context.Background(), bson.M{"_id": groupUUID}, update, options.Update())
	return err
}

// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *Backend) getGroupMeta(groupUUID string) (*tasks.GroupMeta, error) {
	groupMeta := &tasks.GroupMeta{}
	query := bson.M{"_id": groupUUID}

	err := b.groupMetasCollection().FindOne(context.Background(), query).Decode(groupMeta)
	if err != nil {
		return nil, err
	}
	return groupMeta, nil
}

// getStates returns multiple task states
func (b *Backend) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	states := make([]*tasks.TaskState, 0, len(taskUUIDs))
	cur, err := b.tasksCollection().Find(context.Background(), bson.M{"_id": bson.M{"$in": taskUUIDs}})
	if err != nil {
		return nil, err
	}
	defer cur.Close(context.Background())

	for cur.Next(context.Background()) {
		state := &tasks.TaskState{}
		if err := cur.Decode(state); err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	if cur.Err() != nil {
		return nil, err
	}
	return states, nil
}

// updateState saves current task state
func (b *Backend) updateState(signature *tasks.Signature, update bson.M) error {
	update = bson.M{"$set": update}
	_, err := b.tasksCollection().UpdateOne(context.Background(), bson.M{"_id": signature.UUID}, update, options.Update().SetUpsert(true))
	return err
}

func (b *Backend) tasksCollection() *mongo.Collection {
	b.once.Do(func() {
		b.connect()
	})

	return b.tc
}

func (b *Backend) groupMetasCollection() *mongo.Collection {
	b.once.Do(func() {
		b.connect()
	})

	return b.gmc
}

// connect creates the underlying mgo connection if it doesn't exist
// creates required indexes for our collections
func (b *Backend) connect() error {
	client, err := b.dial()
	if err != nil {
		return err
	}
	b.client = client

	database := "machinery"

	if b.GetConfig().MongoDB != nil {
		database = b.GetConfig().MongoDB.Database
	}

	b.tc = b.client.Database(database).Collection("tasks")
	b.gmc = b.client.Database(database).Collection("group_metas")

	err = b.createMongoIndexes(database)
	if err != nil {
		return err
	}
	return nil
}

// dial connects to mongo with TLSConfig if provided
// else connects via ResultBackend uri
func (b *Backend) dial() (*mongo.Client, error) {

	if b.GetConfig().MongoDB != nil && b.GetConfig().MongoDB.Client != nil {
		return b.GetConfig().MongoDB.Client, nil
	}

	uri := b.GetConfig().ResultBackend
	if strings.HasPrefix(uri, "mongodb://") == false &&
		strings.HasPrefix(uri, "mongodb+srv://") == false {
		uri = fmt.Sprintf("mongodb://%s", uri)
	}

	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		return nil, err
	}

	return client, nil
}

// createMongoIndexes ensures all indexes are in place
func (b *Backend) createMongoIndexes(database string) error {

	tasksCollection := b.client.Database(database).Collection("tasks")

	expireIn := int32(b.GetConfig().ResultsExpireIn)

	_, err := tasksCollection.Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys:    bson.M{"state": 1},
			Options: options.Index().SetBackground(true).SetExpireAfterSeconds(expireIn),
		},
		mongo.IndexModel{
			Keys:    bson.M{"lock": 1},
			Options: options.Index().SetBackground(true).SetExpireAfterSeconds(expireIn),
		},
	})
	if err != nil {
		return err
	}

	return err
}
