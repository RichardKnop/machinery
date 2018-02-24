package backends

import (
	"reflect"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// MongodbBackend represents a MongoDB result backend
type MongodbBackend struct {
	Backend
	session              *mgo.Session
	tasksCollection      *mgo.Collection
	groupMetasCollection *mgo.Collection
}

// NewMongodbBackend creates MongodbBackend instance
func NewMongodbBackend(cnf *config.Config) Interface {
	return &MongodbBackend{Backend: New(cnf)}
}

// InitGroup creates and saves a group meta data object
func (b *MongodbBackend) InitGroup(groupUUID string, taskUUIDs []string) error {
	if err := b.connect(); err != nil {
		return err
	}

	groupMeta := &tasks.GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
	}
	return b.groupMetasCollection.Insert(groupMeta)
}

// GroupCompleted returns true if all tasks in a group finished
func (b *MongodbBackend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
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
func (b *MongodbBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return []*tasks.TaskState{}, err
	}

	return b.getStates(groupMeta.TaskUUIDs...)
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *MongodbBackend) TriggerChord(groupUUID string) (bool, error) {
	if err := b.connect(); err != nil {
		return false, err
	}
	query := bson.M{
		"_id":             groupUUID,
		"chord_triggered": false,
	}
	change := mgo.Change{
		Update: bson.M{
			"$set": bson.M{
				"chord_triggered": true,
			},
		},
		ReturnNew: false,
	}
	_, err := b.groupMetasCollection.
		Find(query).
		Apply(change, nil)
	if err != nil {
		if err == mgo.ErrNotFound {
			log.WARNING.Printf("Chord already triggered for group %s", groupUUID)
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// SetStatePending updates task state to PENDING
func (b *MongodbBackend) SetStatePending(signature *tasks.Signature) error {
	update := bson.M{"state": tasks.StatePending}
	return b.updateState(signature, update)
}

// SetStateReceived updates task state to RECEIVED
func (b *MongodbBackend) SetStateReceived(signature *tasks.Signature) error {
	update := bson.M{"state": tasks.StateReceived}
	return b.updateState(signature, update)
}

// SetStateStarted updates task state to STARTED
func (b *MongodbBackend) SetStateStarted(signature *tasks.Signature) error {
	update := bson.M{"state": tasks.StateStarted}
	return b.updateState(signature, update)
}

// SetStateRetry updates task state to RETRY
func (b *MongodbBackend) SetStateRetry(signature *tasks.Signature) error {
	update := bson.M{"state": tasks.StateRetry}
	return b.updateState(signature, update)
}

// SetStateSuccess updates task state to SUCCESS
func (b *MongodbBackend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	//edited by surendra tiwari
	var err error
	bsonResults := make([]bson.M, len(results))
	for i, result := range results {
		//to hold the json result
		bsonResult := new(bson.M)
		resultType := reflect.TypeOf(result.Value).Kind()
		if resultType == reflect.String {
			//convert type to json
			err = bson.UnmarshalJSON([]byte(result.Value.(string)), bsonResult)
			if err == nil {
				bsonResults[i] = bson.M{
					"type":  "Json",
					"value": bsonResult,
				}
			} else {
				bsonResults[i] = bson.M{
					"type":  result.Type,
					"value": result.Value,
				}
			}
		} else {
			bsonResults[i] = bson.M{
				"type":  result.Type,
				"value": result.Value,
			}
		}
	}
	update := bson.M{
		"state":   tasks.StateSuccess,
		"results": bsonResults,
	}
	return b.updateState(signature, update)
}

// SetStateFailure updates task state to FAILURE
func (b *MongodbBackend) SetStateFailure(signature *tasks.Signature, err string) error {
	update := bson.M{"state": tasks.StateFailure, "error": err}
	return b.updateState(signature, update)
}

// GetState returns the latest task state
func (b *MongodbBackend) GetState(taskUUID string) (*tasks.TaskState, error) {
	if err := b.connect(); err != nil {
		return nil, err
	}

	state := new(tasks.TaskState)
	if err := b.tasksCollection.FindId(taskUUID).One(state); err != nil {
		return nil, err
	}
	return state, nil
}

// PurgeState deletes stored task state
func (b *MongodbBackend) PurgeState(taskUUID string) error {
	if err := b.connect(); err != nil {
		return err
	}

	return b.tasksCollection.RemoveId(taskUUID)
}

// PurgeGroupMeta deletes stored group meta data
func (b *MongodbBackend) PurgeGroupMeta(groupUUID string) error {
	if err := b.connect(); err != nil {
		return err
	}

	return b.groupMetasCollection.RemoveId(groupUUID)
}

// lockGroupMeta acquires lock on groupUUID document
func (b *MongodbBackend) lockGroupMeta(groupUUID string) error {
	query := bson.M{
		"_id":  groupUUID,
		"lock": false,
	}
	change := mgo.Change{
		Update: bson.M{
			"$set": bson.M{
				"lock": true,
			},
		},
		Upsert:    true,
		ReturnNew: false,
	}
	_, err := b.groupMetasCollection.
		Find(query).
		Apply(change, nil)
	return err
}

// unlockGroupMeta releases lock on groupUUID document
func (b *MongodbBackend) unlockGroupMeta(groupUUID string) error {
	update := bson.M{"$set": bson.M{"lock": false}}
	_, err := b.groupMetasCollection.UpsertId(groupUUID, update)
	return err
}

// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *MongodbBackend) getGroupMeta(groupUUID string) (*tasks.GroupMeta, error) {
	if err := b.connect(); err != nil {
		return nil, err
	}

	query := bson.M{"_id": groupUUID}

	groupMeta := new(tasks.GroupMeta)
	if err := b.groupMetasCollection.Find(query).One(groupMeta); err != nil {
		return nil, err
	}
	return groupMeta, nil
}

// getStates returns multiple task states
func (b *MongodbBackend) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	if err := b.connect(); err != nil {
		return nil, err
	}

	states := make([]*tasks.TaskState, 0, len(taskUUIDs))

	iter := b.tasksCollection.Find(bson.M{"_id": bson.M{"$in": taskUUIDs}}).Iter()

	state := new(tasks.TaskState)
	for iter.Next(state) {
		states = append(states, state)

		// otherwise we would end up with the last task being every element of the slice
		state = new(tasks.TaskState)
	}

	return states, nil
}

// updateState saves current task state
func (b *MongodbBackend) updateState(signature *tasks.Signature, update bson.M) error {
	if err := b.connect(); err != nil {
		return err
	}

	update = bson.M{"$set": update}
	_, err := b.tasksCollection.UpsertId(signature.UUID, update)
	if err != nil {
		return err
	}
	return nil
}

// connect returns a session if we are already connected to mongo, otherwise
// (when called for the first time) it will open a new session and ensure
// all required indexes for our collections exist
func (b *MongodbBackend) connect() error {
	if b.session != nil {
		return nil
	}

	session, err := mgo.Dial(b.cnf.ResultBackend)
	if err != nil {
		return err
	}
	b.session = session

	b.tasksCollection = b.session.DB("").C("tasks")
	b.groupMetasCollection = b.session.DB("").C("group_metas")

	return b.createMongoIndexes()
}

// createMongoIndexes ensures all indexes are in place
func (b *MongodbBackend) createMongoIndexes() error {
	indexes := []mgo.Index{
		{
			Key:         []string{"state"},
			Background:  true, // can be used while index is being built
			ExpireAfter: time.Duration(b.cnf.ResultsExpireIn) * time.Second,
		},
		{
			Key:         []string{"lock"},
			Background:  true, // can be used while index is being built
			ExpireAfter: time.Duration(b.cnf.ResultsExpireIn) * time.Second,
		},
	}

	for _, index := range indexes {
		// Check if index already exists, if it does, skip
		if err := b.tasksCollection.EnsureIndex(index); err == nil {
			log.INFO.Printf("%s index already exist, skipping create step", index.Key[0])
			continue
		}

		// Create index (keep in mind EnsureIndex is blocking operation)
		log.INFO.Printf("Creating %s index", index.Key[0])
		if err := b.tasksCollection.DropIndex(index.Key[0]); err != nil {
			return err
		}
		if err := b.tasksCollection.EnsureIndex(index); err != nil {
			return err
		}
	}

	return nil
}
