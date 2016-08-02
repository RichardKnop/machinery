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
	config          *config.Config
	mongoCollection *mgo.Collection
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

	mongoCollection := session.DB(dbName).C("tasks")

	err = createMongoIndexes(mongoCollection, conf)
	if err != nil {
		return nil, err
	}

	return Backend(&MongodbBackend{
		config:          conf,
		mongoCollection: mongoCollection,
	}), nil
}

// Group related functions

// InitGroup - saves UUIDs of all tasks in a group
func (m *MongodbBackend) InitGroup(groupUUID string, taskUUIDs []string) error {
	var task *mongodbTask
	var err error

	for _, taskUUID := range taskUUIDs {
		task = &mongodbTask{
			TaskUUID:   taskUUID,
			GroupUUID:  groupUUID,
			CreateTime: time.Now(),
		}
		err = m.mongoCollection.Insert(task)
		if err != nil {
			return err
		}
	}
	return nil
}

// GroupCompleted - returns true if all tasks in a group finished
func (m *MongodbBackend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	iter := m.mongoCollection.Find(bson.M{"group_uuid": groupUUID}).Iter()

	var task mongodbTask
	var countSuccessTasks = 0
	for iter.Next(&task) {
		if !task.TaskState().IsCompleted() {
			return false, nil
		}
		countSuccessTasks++
	}
	return countSuccessTasks == groupTaskCount, nil
}

// GroupTaskStates - returns states of all tasks in the group
func (m *MongodbBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*TaskState, error) {
	iter := m.mongoCollection.Find(bson.M{"group_uuid": groupUUID}).Iter()

	taskStates := make([]*TaskState, 0, groupTaskCount)

	var task mongodbTask
	for iter.Next(&task) {
		taskStates = append(taskStates, task.TaskState())
	}

	return taskStates, nil
}

// Setting / getting task state

// SetStatePending - sets task state to PENDING
func (m *MongodbBackend) SetStatePending(signature *signatures.TaskSignature) error {
	update := bson.M{"state": PendingState}
	return m.setState(signature, update)
}

// SetStateReceived - sets task state to RECEIVED
func (m *MongodbBackend) SetStateReceived(signature *signatures.TaskSignature) error {
	update := bson.M{"state": ReceivedState}
	return m.setState(signature, update)
}

// SetStateStarted - sets task state to STARTED
func (m *MongodbBackend) SetStateStarted(signature *signatures.TaskSignature) error {
	update := bson.M{"state": StartedState}
	return m.setState(signature, update)
}

// SetStateSuccess - sets task state to SUCCESS
func (m *MongodbBackend) SetStateSuccess(signature *signatures.TaskSignature, result *TaskResult) error {
	update := bson.M{
		"state": SuccessState,
		"result": bson.M{
			"type":  result.Type,
			"value": result.Value,
		},
	}
	return m.setState(signature, update)
}

// SetStateFailure - sets task state to FAILURE
func (m *MongodbBackend) SetStateFailure(signature *signatures.TaskSignature, err string) error {
	update := bson.M{"state": FailureState, "error": err}
	return m.setState(signature, update)
}

// GetState - returns the latest task state
func (m *MongodbBackend) GetState(taskUUID string) (*TaskState, error) {
	var task mongodbTask
	err := m.mongoCollection.Find(bson.M{"_id": taskUUID}).One(&task)
	if err != nil {
		return nil, err
	}
	return task.TaskState(), nil
}

// Purging stored stored tasks states and group meta data

// PurgeState - deletes stored task state
func (m *MongodbBackend) PurgeState(taskUUID string) error {
	return m.mongoCollection.RemoveId(taskUUID)
}

// PurgeGroupMeta - deletes stored group meta data
func (m *MongodbBackend) PurgeGroupMeta(groupUUID string) error {
	_, err := m.mongoCollection.RemoveAll(bson.M{"group_uuid": groupUUID})
	if err != nil {
		return err
	}
	return nil
}

func (m *MongodbBackend) setState(signature *signatures.TaskSignature, update bson.M) error {
	newTask := bson.M{
		"group_uuid": signature.GroupUUID,
		"createdAt":  time.Now(),
	}
	_, err := m.mongoCollection.Upsert(bson.M{"_id": signature.UUID}, bson.M{"$set": update, "$setOnInsert": newTask})
	if err != nil {
		return err
	}
	return nil
}

func createMongoIndexes(collection *mgo.Collection, conf *config.Config) error {
	indexGroupUUID := mgo.Index{
		Key: []string{"group_uuid"},
	}
	indexCreatedAt := mgo.Index{
		Key:         []string{"createdAt"},
		ExpireAfter: time.Duration(conf.ResultsExpireIn) * time.Second,
	}

	var err error
	err = collection.EnsureIndex(indexGroupUUID)
	if err != nil {
		err = collection.DropIndex(indexGroupUUID.Key[0])
		if err != nil {
			return err
		}
		err = collection.EnsureIndex(indexGroupUUID)
		if err != nil {
			return err
		}
	}

	err = collection.EnsureIndex(indexCreatedAt)
	if err != nil {
		err = collection.DropIndex(indexCreatedAt.Key[0])
		if err != nil {
			return err
		}
		err = collection.EnsureIndex(indexCreatedAt)
		if err != nil {
			return err
		}
	}
	return nil
}

type mongodbTask struct {
	TaskUUID   string            `bson:"_id"`
	GroupUUID  string            `bson:"group_uuid"`
	CreateTime time.Time         `bson:"createdAt"`
	State      string            `bson:"state"`
	Result     mongodbTaskResult `bson:"result"`
	Error      string            `bson:"error"`
}

func (m *mongodbTask) TaskState() *TaskState {
	taskState := &TaskState{
		TaskUUID: m.TaskUUID,
		State:    m.State,
	}

	if taskState.State == SuccessState {
		taskState.Result = m.Result.TaskResult()
	} else if taskState.State == FailureState {
		taskState.Error = m.Error
	}
	return taskState
}

type mongodbTaskResult struct {
	Type  string      `bson:"type"`
	Value interface{} `bson:"value"`
}

func (m *mongodbTaskResult) TaskResult() *TaskResult {
	var value interface{}
	value = m.Value

	if strings.HasPrefix(m.Type, "int") {
		value = float64(value.(int64))
	} else if strings.HasPrefix(m.Type, "uint") {
		value = float64(value.(uint64))
	} else if strings.HasPrefix(m.Type, "float") {
		value = float64(value.(float64))
	}

	return &TaskResult{
		Type:  m.Type,
		Value: value,
	}
}
