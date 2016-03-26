package backends

import (
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
)

var (
	MongoDBConnectionString = os.Getenv("MONGODB_URL")

	groupUUID = "123456"
	taskUUIDs = []string{"1", "2", "3"}
)

func initTestMongodbBackend() (Backend, error) {
	conf := &config.Config{
		ResultBackend:   MongoDBConnectionString,
		ResultsExpireIn: 30,
	}
	backend, err := NewMongodbBackend(conf)
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
	if err != nil {
		t.Fatal(err)
	}
	if backend == nil {
		t.Fatal("MongodbBackend is nil")
	}
}

func TestEmptyState(t *testing.T) {
	backend, _ := initTestMongodbBackend()

	taskState, err := backend.GetState(taskUUIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if taskState.State != "" {
		t.Fatal("Not empty taskState")
	}
}

func TestSetStatePending(t *testing.T) {
	backend, _ := initTestMongodbBackend()

	err := backend.SetStatePending(&signatures.TaskSignature{
		UUID: taskUUIDs[0],
	})
	if err != nil {
		t.Fatal(err)
	}

	taskState, err := backend.GetState(taskUUIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if taskState.State != PendingState {
		t.Fatal("Not PendingState")
	}
}

func TestSetStateReceived(t *testing.T) {
	backend, _ := initTestMongodbBackend()

	err := backend.SetStateReceived(&signatures.TaskSignature{
		UUID: taskUUIDs[0],
	})
	if err != nil {
		t.Fatal(err)
	}

	taskState, err := backend.GetState(taskUUIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if taskState.State != ReceivedState {
		t.Fatal("Not ReceivedState")
	}
}

func TestSetStateStarted(t *testing.T) {
	backend, _ := initTestMongodbBackend()

	err := backend.SetStateStarted(&signatures.TaskSignature{
		UUID: taskUUIDs[0],
	})
	if err != nil {
		t.Fatal(err)
	}

	taskState, err := backend.GetState(taskUUIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if taskState.State != StartedState {
		t.Fatal("Not StartedState")
	}
}

func TestSetStateSuccess(t *testing.T) {
	resultType := "int64"
	resultValue := int64(88)

	backend, _ := initTestMongodbBackend()

	err := backend.SetStateSuccess(&signatures.TaskSignature{
		UUID: taskUUIDs[0],
	}, &TaskResult{
		Type:  resultType,
		Value: resultValue,
	})
	if err != nil {
		t.Fatal(err)
	}

	taskState, err := backend.GetState(taskUUIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if taskState.State != SuccessState {
		t.Fatal("Not SuccessState")
	}
	if taskState.Result.Type != resultType {
		t.Fatal("Wrong result type")
	}
	if taskState.Result.Value.(float64) != float64(resultValue) {
		t.Fatal("Wrong result value")
	}
}

func TestSetStateFailure(t *testing.T) {
	failStrig := "Fail is ok"

	backend, _ := initTestMongodbBackend()

	err := backend.SetStateFailure(&signatures.TaskSignature{
		UUID: taskUUIDs[0],
	}, failStrig)
	if err != nil {
		t.Fatal(err)
	}

	taskState, err := backend.GetState(taskUUIDs[0])
	if err != nil {
		t.Fatal(err)
	}
	if taskState.State != FailureState {
		t.Fatal("Not FailureState")
	}
	if taskState.Error != failStrig {
		t.Fatal("Wrong fail error")
	}
}

func TestGroupCompleted(t *testing.T) {
	backend, _ := initTestMongodbBackend()
	taskResultsState := make(map[string]string)

	isCompleted, err := backend.GroupCompleted(groupUUID, len(taskUUIDs))
	if err != nil {
		t.Fatal(err)
	}
	if isCompleted {
		t.Fatal("Actualy group is not completed")
	}

	err = backend.SetStateFailure(&signatures.TaskSignature{
		UUID: taskUUIDs[0],
	}, "Fail is ok")
	if err != nil {
		t.Fatal(err)
	}
	taskResultsState[taskUUIDs[0]] = FailureState

	err = backend.SetStateSuccess(&signatures.TaskSignature{
		UUID: taskUUIDs[1],
	}, &TaskResult{
		Type:  "string",
		Value: "Result ok",
	})
	if err != nil {
		t.Fatal(err)
	}
	taskResultsState[taskUUIDs[1]] = SuccessState

	err = backend.SetStateSuccess(&signatures.TaskSignature{
		UUID: taskUUIDs[2],
	}, &TaskResult{
		Type:  "string",
		Value: "Result ok",
	})
	if err != nil {
		t.Fatal(err)
	}
	taskResultsState[taskUUIDs[2]] = SuccessState

	isCompleted, err = backend.GroupCompleted(groupUUID, len(taskUUIDs))
	if err != nil {
		t.Fatal(err)
	}
	if !isCompleted {
		t.Fatal("Actualy group is completed")
	}

	groupTasksStates, err := backend.GroupTaskStates(groupUUID, len(taskUUIDs))
	if err != nil {
		t.Fatal(err)
	}
	if len(groupTasksStates) != len(taskUUIDs) {
		t.Fatal("Wrong len tasksStates")
	}
	for i := range groupTasksStates {
		if groupTasksStates[i].State != taskResultsState[groupTasksStates[i].TaskUUID] {
			t.Fatal("Wrong state on", groupTasksStates[i].TaskUUID)
		}
	}
}

func TestMongodbDropIndexes(t *testing.T) {
	conf := &config.Config{
		ResultBackend:   MongoDBConnectionString,
		ResultsExpireIn: 5,
	}

	_, err := NewMongodbBackend(conf)
	if err != nil {
		t.Fatal(err)
	}

	conf.ResultsExpireIn = 7

	_, err = NewMongodbBackend(conf)
	if err != nil {
		t.Fatal(err)
	}
}
