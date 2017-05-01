package backends

import (
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
)

// MongodbBackend represents a MongoDB result backend
type MongodbBackend struct {
	config               *config.Config
	session              *mgo.Session
	tasksCollection      *mgo.Collection
	groupMetasCollection *mgo.Collection
}

// NewMongodbBackend creates MongodbBackend instance
func NewMongodbBackend(conf *config.Config) (Backend, error) {
	session, err := mgo.Dial(conf.ResultBackend)
	if err != nil {
		return nil, err
	}

	dbName := "tasks"
	splitConnection := strings.Split(conf.ResultBackend, "/")
	if len(splitConnection) == 4 {
		dbName = splitConnection[3]
	}

	tasksCollection := session.DB(dbName).C("tasks")
	groupMetasCollection := session.DB(dbName).C("group_metas")

	err = createMongoIndexes(tasksCollection, conf)
	if err != nil {
		return nil, err
	}

	return Backend(&MongodbBackend{
		config:               conf,
		session:              session,
		tasksCollection:      tasksCollection,
		groupMetasCollection: groupMetasCollection,
	}), nil
}

// InitGroup - saves UUIDs of all tasks in a group
func (b *MongodbBackend) InitGroup(groupUUID string, taskUUIDs []string) error {
	groupMeta := &mongodbGroupMeta{
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
func (b *MongodbBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*TaskState, error) {
	taskStates := make([]*TaskState, groupTaskCount)

	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return taskStates, err
	}

	return b.getStates(groupMeta.TaskUUIDs...)
}

// TriggerChord - marks chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *MongodbBackend) TriggerChord(groupUUID string) (bool, error) {
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
func (b *MongodbBackend) SetStatePending(signature *signatures.TaskSignature) error {
	update := bson.M{"state": PendingState}
	return b.setState(signature, update)
}

// SetStateReceived - sets task state to RECEIVED
func (b *MongodbBackend) SetStateReceived(signature *signatures.TaskSignature) error {
	update := bson.M{"state": ReceivedState}
	return b.setState(signature, update)
}

// SetStateStarted - sets task state to STARTED
func (b *MongodbBackend) SetStateStarted(signature *signatures.TaskSignature) error {
	update := bson.M{"state": StartedState}
	return b.setState(signature, update)
}

// SetStateSuccess - sets task state to SUCCESS
func (b *MongodbBackend) SetStateSuccess(signature *signatures.TaskSignature, results []*TaskResult) error {
	bsonResults := make([]bson.M, len(results))
	for i, result := range results {
		bsonResults[i] = bson.M{
			"type":  result.Type,
			"value": result.Value,
		}
	}
	update := bson.M{
		"state":   SuccessState,
		"results": bsonResults,
	}
	return b.setState(signature, update)
}

// SetStateFailure - sets task state to FAILURE
func (b *MongodbBackend) SetStateFailure(signature *signatures.TaskSignature, err string) error {
	update := bson.M{"state": FailureState, "error": err}
	return b.setState(signature, update)
}

// GetState - returns the latest task state
func (b *MongodbBackend) GetState(taskUUID string) (*TaskState, error) {
	task := new(mongodbTaskState)
	if err := b.tasksCollection.FindId(taskUUID).One(&task); err != nil {
		return nil, err
	}
	return task.TaskState(), nil
}

// PurgeState - deletes stored task state
func (b *MongodbBackend) PurgeState(taskUUID string) error {
	return b.tasksCollection.RemoveId(taskUUID)
}

// PurgeGroupMeta - deletes stored group meta data
func (b *MongodbBackend) PurgeGroupMeta(groupUUID string) error {
	return b.groupMetasCollection.RemoveId(groupUUID)
}

// Fetches GroupMeta from the backend, convenience function to avoid repetition
func (b *MongodbBackend) getGroupMeta(groupUUID string) (*GroupMeta, error) {
	groupMeta := new(mongodbGroupMeta)
	if err := b.groupMetasCollection.FindId(groupUUID).One(groupMeta); err != nil {
		return nil, err
	}
	return groupMeta.GroupMeta(), nil
}

// getStates Returns multiple task states with MGET
func (b *MongodbBackend) getStates(taskUUIDs ...string) ([]*TaskState, error) {
	taskStates := make([]*TaskState, 0, len(taskUUIDs))

	iter := b.tasksCollection.Find(bson.M{"_id": bson.M{"$in": taskUUIDs}}).Iter()

	task := new(mongodbTaskState)
	for iter.Next(task) {
		taskStates = append(taskStates, task.TaskState())
	}

	return taskStates, nil
}

func (b *MongodbBackend) setState(signature *signatures.TaskSignature, update bson.M) error {
	newTask := bson.M{
		"createdAt": time.Now(),
	}
	_, err := b.tasksCollection.UpsertId(signature.UUID, bson.M{"$set": update, "$setOnInsert": newTask})
	if err != nil {
		return err
	}
	return nil
}

func createMongoIndexes(tasksCollection *mgo.Collection, conf *config.Config) error {
	indexCreatedAt := mgo.Index{
		Key:         []string{"createdAt"},
		ExpireAfter: time.Duration(conf.ResultsExpireIn) * time.Second,
	}

	if err := tasksCollection.EnsureIndex(indexCreatedAt); err != nil {
		if err = tasksCollection.DropIndex(indexCreatedAt.Key[0]); err != nil {
			return err
		}
		if err = tasksCollection.EnsureIndex(indexCreatedAt); err != nil {
			return err
		}
	}

	return nil
}
