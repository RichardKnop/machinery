package mongo

import (
	"crypto/tls"
	"encoding/json"
	"net"
	"reflect"
	"strings"
	"time"

	"github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// Backend represents a MongoDB result backend
type Backend struct {
	common.Backend
	session *mgo.Session
}

// New creates Backend instance
func New(cnf *config.Config) iface.Backend {
	return &Backend{Backend: common.NewBackend(cnf)}
}

// Op represents a mongo operation using a copied session
type Op struct {
	session              *mgo.Session
	tasksCollection      *mgo.Collection
	groupMetasCollection *mgo.Collection
}

// Do wraps a func using op & defers session close
func (op *Op) Do(f func() error) error {
	defer op.session.Close()
	return f()
}

// newOp returns an Op pointer w/ copied session
// and task & groupMetas collections
func (b *Backend) newOp() *Op {
	session := b.session.Copy()
	return &Op{
		session:              session,
		tasksCollection:      session.DB("").C("tasks"),
		groupMetasCollection: session.DB("").C("group_metas"),
	}
}

// InitGroup creates and saves a group meta data object
func (b *Backend) InitGroup(groupUUID string, taskUUIDs []string) error {
	op, err := b.connect()
	if err != nil {
		return err
	}
	return op.Do(func() error {
		groupMeta := &tasks.GroupMeta{
			GroupUUID: groupUUID,
			TaskUUIDs: taskUUIDs,
			CreatedAt: time.Now().UTC(),
		}
		return op.groupMetasCollection.Insert(groupMeta)
	})
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
	op, err := b.connect()
	if err != nil {
		return false, err
	}
	err = op.Do(func() error {
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
		_, err := op.groupMetasCollection.
			Find(query).
			Apply(change, nil)
		return err
	})
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
	op, err := b.connect()
	if err != nil {
		return nil, err
	}
	state := new(tasks.TaskState)
	err = op.Do(func() error {
		return op.tasksCollection.FindId(taskUUID).One(state)
	})
	if err != nil {
		return nil, err
	}
	return state, nil
}

// PurgeState deletes stored task state
func (b *Backend) PurgeState(taskUUID string) error {
	op, err := b.connect()
	if err != nil {
		return err
	}
	return op.Do(func() error {
		return op.tasksCollection.RemoveId(taskUUID)
	})
}

// PurgeGroupMeta deletes stored group meta data
func (b *Backend) PurgeGroupMeta(groupUUID string) error {
	op, err := b.connect()
	if err != nil {
		return err
	}
	return op.Do(func() error {
		return op.groupMetasCollection.RemoveId(groupUUID)
	})
}

// lockGroupMeta acquires lock on groupUUID document
func (b *Backend) lockGroupMeta(groupUUID string) error {
	op, err := b.connect()
	if err != nil {
		return err
	}
	return op.Do(func() error {
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
		_, err := op.groupMetasCollection.
			Find(query).
			Apply(change, nil)
		return err
	})
}

// unlockGroupMeta releases lock on groupUUID document
func (b *Backend) unlockGroupMeta(groupUUID string) error {
	op, err := b.connect()
	if err != nil {
		return err
	}
	return op.Do(func() error {
		update := bson.M{"$set": bson.M{"lock": false}}
		_, err := op.groupMetasCollection.UpsertId(groupUUID, update)
		return err
	})
}

// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *Backend) getGroupMeta(groupUUID string) (*tasks.GroupMeta, error) {
	op, err := b.connect()
	if err != nil {
		return nil, err
	}
	groupMeta := new(tasks.GroupMeta)
	err = op.Do(func() error {
		query := bson.M{"_id": groupUUID}
		return op.groupMetasCollection.Find(query).One(groupMeta)
	})
	if err != nil {
		return nil, err
	}
	return groupMeta, nil
}

// getStates returns multiple task states
func (b *Backend) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	op, err := b.connect()
	if err != nil {
		return nil, err
	}
	states := make([]*tasks.TaskState, 0, len(taskUUIDs))
	op.Do(func() error {
		iter := op.tasksCollection.Find(bson.M{"_id": bson.M{"$in": taskUUIDs}}).Iter()
		state := new(tasks.TaskState)
		for iter.Next(state) {
			states = append(states, state)
			// otherwise we would end up with the last task being every element of the slice
			state = new(tasks.TaskState)
		}
		return nil
	})
	return states, nil
}

// updateState saves current task state
func (b *Backend) updateState(signature *tasks.Signature, update bson.M) error {
	op, err := b.connect()
	if err != nil {
		return err
	}
	return op.Do(func() error {
		update = bson.M{"$set": update}
		_, err := op.tasksCollection.UpsertId(signature.UUID, update)
		return err
	})
}

// connect creates the underlying mgo session if it doesn't exist
// creates required indexes for our collections
// and returns a a new Op
func (b *Backend) connect() (*Op, error) {
	if b.session != nil {
		b.session.Refresh()
		return b.newOp(), nil
	}
	session, err := b.dial()
	if err != nil {
		return nil, err
	}
	b.session = session
	err = b.createMongoIndexes()
	if err != nil {
		return nil, err
	}
	return b.newOp(), nil
}

// dial connects to mongo with TLSConfig if provided
// else connects via ResultBackend uri
func (b *Backend) dial() (*mgo.Session, error) {
	if b.GetConfig().TLSConfig == nil {
		return mgo.Dial(b.GetConfig().ResultBackend)
	}
	dialInfo, err := mgo.ParseURL(b.GetConfig().ResultBackend)
	if err != nil {
		return nil, err
	}
	dialInfo.Timeout = 5 * time.Second
	dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
		return tls.Dial("tcp", addr.String(), b.GetConfig().TLSConfig)
	}
	return mgo.DialWithInfo(dialInfo)
}

// createMongoIndexes ensures all indexes are in place
func (b *Backend) createMongoIndexes() error {
	op, err := b.connect()
	if err != nil {
		return err
	}
	return op.Do(func() error {
		indexes := []mgo.Index{
			{
				Key:         []string{"state"},
				Background:  true, // can be used while index is being built
				ExpireAfter: time.Duration(b.GetConfig().ResultsExpireIn) * time.Second,
			},
			{
				Key:         []string{"lock"},
				Background:  true, // can be used while index is being built
				ExpireAfter: time.Duration(b.GetConfig().ResultsExpireIn) * time.Second,
			},
		}

		for _, index := range indexes {
			// Check if index already exists, if it does, skip
			if err := op.tasksCollection.EnsureIndex(index); err == nil {
				log.INFO.Printf("%s index already exist, skipping create step", index.Key[0])
				continue
			}

			// Create index (keep in mind EnsureIndex is blocking operation)
			log.INFO.Printf("Creating %s index", index.Key[0])
			if err := op.tasksCollection.DropIndex(index.Key[0]); err != nil {
				return err
			}
			if err := op.tasksCollection.EnsureIndex(index); err != nil {
				return err
			}
		}
		return nil
	})
}
