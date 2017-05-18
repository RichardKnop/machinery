package backends

import (
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
)

// MongodbBackend represents a MongoDB result backend
type MongodbBackend struct {
	cnf                  *config.Config
	session              *mgo.Session
	tasksCollection      *mgo.Collection
	groupMetasCollection *mgo.Collection
}

// NewMongodbBackend creates MongodbBackend instance
func NewMongodbBackend(cnf *config.Config) Interface {
	return &MongodbBackend{cnf: cnf}
}

// InitGroup - saves UUIDs of all tasks in a group
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

// GroupCompleted - returns true if all tasks in a group finished
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

// GroupTaskStates - returns states of all tasks in the group
func (b *MongodbBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return []*tasks.TaskState{}, err
	}

	return b.getStates(groupMeta.TaskUUIDs...)
}

// TriggerChord - marks chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *MongodbBackend) TriggerChord(groupUUID string) (bool, error) {
	if err := b.connect(); err != nil {
		return false, err
	}

	if err := b.session.FsyncLock(); err != nil {
		return false, err
	}
	defer b.session.FsyncUnlock()

	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	// Chord has already been triggered, return false (should not trigger again)
	if groupMeta.ChordTriggered {
		return false, nil
	}

	// Set flag to true
	groupMeta.ChordTriggered = true

	// Update the group meta
	update := bson.M{
		"chord_triggered": true,
	}
	_, err = b.tasksCollection.UpsertId(groupUUID, bson.M{"$set": update})
	if err != nil {
		return false, err
	}

	return true, nil
}

// SetStatePending - sets task state to PENDING
func (b *MongodbBackend) SetStatePending(signature *tasks.Signature) error {
	update := bson.M{"state": tasks.PendingState}
	return b.updateState(signature, update)
}

// SetStateReceived - sets task state to RECEIVED
func (b *MongodbBackend) SetStateReceived(signature *tasks.Signature) error {
	update := bson.M{"state": tasks.ReceivedState}
	return b.updateState(signature, update)
}

// SetStateStarted - sets task state to STARTED
func (b *MongodbBackend) SetStateStarted(signature *tasks.Signature) error {
	update := bson.M{"state": tasks.StartedState}
	return b.updateState(signature, update)
}

// SetStateSuccess - sets task state to SUCCESS
func (b *MongodbBackend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	bsonResults := make([]bson.M, len(results))
	for i, result := range results {
		bsonResults[i] = bson.M{
			"type":  result.Type,
			"value": result.Value,
		}
	}
	update := bson.M{
		"state":   tasks.SuccessState,
		"results": bsonResults,
	}
	return b.updateState(signature, update)
}

// SetStateFailure - sets task state to FAILURE
func (b *MongodbBackend) SetStateFailure(signature *tasks.Signature, err string) error {
	update := bson.M{"state": tasks.FailureState, "error": err}
	return b.updateState(signature, update)
}

// GetState - returns the latest task state
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

// PurgeState - deletes stored task state
func (b *MongodbBackend) PurgeState(taskUUID string) error {
	if err := b.connect(); err != nil {
		return err
	}

	return b.tasksCollection.RemoveId(taskUUID)
}

// PurgeGroupMeta - deletes stored group meta data
func (b *MongodbBackend) PurgeGroupMeta(groupUUID string) error {
	if err := b.connect(); err != nil {
		return err
	}

	return b.groupMetasCollection.RemoveId(groupUUID)
}

// Fetches GroupMeta from the backend, convenience function to avoid repetition
func (b *MongodbBackend) getGroupMeta(groupUUID string) (*tasks.GroupMeta, error) {
	if err := b.connect(); err != nil {
		return nil, err
	}

	groupMeta := new(tasks.GroupMeta)
	if err := b.groupMetasCollection.FindId(groupUUID).One(groupMeta); err != nil {
		return nil, err
	}
	return groupMeta, nil
}

// getStates Returns multiple task states with MGET
func (b *MongodbBackend) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	if err := b.connect(); err != nil {
		return nil, err
	}

	states := make([]*tasks.TaskState, 0, len(taskUUIDs))

	iter := b.tasksCollection.Find(bson.M{"_id": bson.M{"$in": taskUUIDs}}).Iter()

	state := new(tasks.TaskState)
	for iter.Next(state) {
		states = append(states, state)
	}

	return states, nil
}

func (b *MongodbBackend) updateState(signature *tasks.Signature, update bson.M) error {
	if err := b.connect(); err != nil {
		return err
	}

	_, err := b.tasksCollection.UpsertId(signature.UUID, bson.M{"$set": update})
	if err != nil {
		return err
	}
	return nil
}

func (b *MongodbBackend) connect() error {
	if b.session != nil {
		return nil
	}

	session, err := mgo.Dial(b.cnf.ResultBackend)
	if err != nil {
		return err
	}
	b.session = session

	dbName := "tasks"
	splitConnection := strings.Split(b.cnf.ResultBackend, "/")
	if len(splitConnection) == 4 {
		dbName = splitConnection[3]
	}

	b.tasksCollection = session.DB(dbName).C("tasks")
	b.groupMetasCollection = session.DB(dbName).C("group_metas")

	return createMongoIndexes(b.tasksCollection, b.cnf)
}

func createMongoIndexes(tasksCollection *mgo.Collection, cnf *config.Config) error {
	indexState := mgo.Index{
		Key:         []string{"state"},
		ExpireAfter: time.Duration(cnf.ResultsExpireIn) * time.Second,
	}

	if err := tasksCollection.EnsureIndex(indexState); err != nil {
		if err = tasksCollection.DropIndex(indexState.Key[0]); err != nil {
			return err
		}
		if err = tasksCollection.EnsureIndex(indexState); err != nil {
			return err
		}
	}

	return nil
}
