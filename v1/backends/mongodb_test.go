package backends_test

import (
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v1/backends"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/stretchr/testify/assert"
)

var (
	MongoDBConnectionString = os.Getenv("MONGODB_URL")

	groupUUID = "123456"
	taskUUIDs = []string{"1", "2", "3"}
)

func initTestMongodbBackend() (backends.Backend, error) {
	conf := &config.Config{
		ResultBackend:   MongoDBConnectionString,
		ResultsExpireIn: 30,
	}
	backend, err := backends.NewMongodbBackend(conf)
	if err != nil {
		return nil, err
	}

	err = backend.PurgeGroupMeta(groupUUID)
	if err != nil {
		return nil, err
	}
	err = backend.InitGroup(groupUUID, taskUUIDs)
	if err != nil {
		return nil, err
	}
	return backend, nil
}

func TestNewMongodbBackend(t *testing.T) {
	backend, err := initTestMongodbBackend()
	if assert.NoError(t, err) {
		assert.NotNil(t, backend)
	}
}

func TestEmptyState(t *testing.T) {
	backend, _ := initTestMongodbBackend()

	taskState, err := backend.GetState(taskUUIDs[0])
	if assert.NoError(t, err) {
		assert.Equal(t, "", taskState.State, "Not empty state")
	}
}

func TestSetStatePending(t *testing.T) {
	backend, _ := initTestMongodbBackend()

	err := backend.SetStatePending(&signatures.TaskSignature{
		UUID: taskUUIDs[0],
	})
	if assert.NoError(t, err) {
		taskState, err := backend.GetState(taskUUIDs[0])
		if assert.NoError(t, err) {
			assert.Equal(t, backends.PendingState, taskState.State, "Not PendingState")
		}
	}
}

func TestSetStateReceived(t *testing.T) {
	backend, _ := initTestMongodbBackend()

	err := backend.SetStateReceived(&signatures.TaskSignature{
		UUID: taskUUIDs[0],
	})
	if assert.NoError(t, err) {
		taskState, err := backend.GetState(taskUUIDs[0])
		if assert.NoError(t, err) {
			assert.Equal(t, backends.ReceivedState, taskState.State, "Not ReceivedState")
		}
	}
}

func TestSetStateStarted(t *testing.T) {
	backend, _ := initTestMongodbBackend()

	err := backend.SetStateStarted(&signatures.TaskSignature{
		UUID: taskUUIDs[0],
	})
	if assert.NoError(t, err) {
		taskState, err := backend.GetState(taskUUIDs[0])
		if assert.NoError(t, err) {
			assert.Equal(t, backends.StartedState, taskState.State, "Not StartedState")
		}
	}
}

func TestSetStateSuccess(t *testing.T) {
	resultType := "int64"
	resultValue := int64(88)

	backend, _ := initTestMongodbBackend()

	err := backend.SetStateSuccess(&signatures.TaskSignature{
		UUID: taskUUIDs[0],
	}, &backends.TaskResult{
		Type:  resultType,
		Value: resultValue,
	})
	if assert.NoError(t, err) {
		taskState, err := backend.GetState(taskUUIDs[0])
		if assert.NoError(t, err) {
			assert.Equal(t, backends.SuccessState, taskState.State, "Not SuccessState")
			assert.Equal(t, resultType, taskState.Result.Type, "Wrong result type")
			assert.Equal(t, float64(resultValue), taskState.Result.Value.(float64), "Wrong result value")
		}
	}
}

func TestSetStateFailure(t *testing.T) {
	failStrig := "Fail is ok"

	backend, _ := initTestMongodbBackend()

	err := backend.SetStateFailure(&signatures.TaskSignature{
		UUID: taskUUIDs[0],
	}, failStrig)
	if assert.NoError(t, err) {
		taskState, err := backend.GetState(taskUUIDs[0])
		if assert.NoError(t, err) {
			assert.Equal(t, backends.FailureState, taskState.State, "Not SuccessState")
			assert.Equal(t, failStrig, taskState.Error, "Wrong fail error")
		}
	}
}

func TestGroupCompleted(t *testing.T) {
	backend, _ := initTestMongodbBackend()
	taskResultsState := make(map[string]string)

	isCompleted, err := backend.GroupCompleted(groupUUID, len(taskUUIDs))
	if assert.NoError(t, err) {
		assert.False(t, isCompleted, "Actualy group is not completed")
	}

	err = backend.SetStateFailure(&signatures.TaskSignature{
		UUID: taskUUIDs[0],
	}, "Fail is ok")
	assert.NoError(t, err)
	taskResultsState[taskUUIDs[0]] = backends.FailureState

	err = backend.SetStateSuccess(&signatures.TaskSignature{
		UUID: taskUUIDs[1],
	}, &backends.TaskResult{
		Type:  "string",
		Value: "Result ok",
	})
	assert.NoError(t, err)
	taskResultsState[taskUUIDs[1]] = backends.SuccessState

	err = backend.SetStateSuccess(&signatures.TaskSignature{
		UUID: taskUUIDs[2],
	}, &backends.TaskResult{
		Type:  "string",
		Value: "Result ok",
	})
	assert.NoError(t, err)
	taskResultsState[taskUUIDs[2]] = backends.SuccessState

	isCompleted, err = backend.GroupCompleted(groupUUID, len(taskUUIDs))
	if assert.NoError(t, err) {
		assert.True(t, isCompleted, "Actualy group is completed")
	}

	groupTasksStates, err := backend.GroupTaskStates(groupUUID, len(taskUUIDs))
	if assert.NoError(t, err) {
		assert.Equal(t, len(groupTasksStates), len(taskUUIDs), "Wrong len tasksStates")
		for i := range groupTasksStates {
			assert.Equal(
				t,
				taskResultsState[groupTasksStates[i].TaskUUID],
				groupTasksStates[i].State,
				"Wrong state on", groupTasksStates[i].TaskUUID,
			)
		}
	}
}

func TestMongodbDropIndexes(t *testing.T) {
	conf := &config.Config{
		ResultBackend:   MongoDBConnectionString,
		ResultsExpireIn: 5,
	}

	_, err := backends.NewMongodbBackend(conf)
	assert.NoError(t, err)

	conf.ResultsExpireIn = 7

	_, err = backends.NewMongodbBackend(conf)
	assert.NoError(t, err)
}
