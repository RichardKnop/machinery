package integrationtests

import (
	"reflect"
	"sort"
	"testing"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/errors"
	"github.com/RichardKnop/machinery/v1/signatures"
)

type ascendingInt64s []int64

func (a ascendingInt64s) Len() int           { return len(a) }
func (a ascendingInt64s) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ascendingInt64s) Less(i, j int) bool { return a[i] < a[j] }

func _getTasks() []signatures.TaskSignature {
	task0 := signatures.TaskSignature{
		Name: "add",
		Args: []signatures.TaskArg{
			{
				Type:  "int64",
				Value: 1,
			},
			{
				Type:  "int64",
				Value: 1,
			},
		},
	}

	task1 := signatures.TaskSignature{
		Name: "add",
		Args: []signatures.TaskArg{
			{
				Type:  "int64",
				Value: 2,
			},
			{
				Type:  "int64",
				Value: 2,
			},
		},
	}

	task2 := signatures.TaskSignature{
		Name: "add",
		Args: []signatures.TaskArg{
			{
				Type:  "int64",
				Value: 5,
			},
			{
				Type:  "int64",
				Value: 6,
			},
		},
	}

	task3 := signatures.TaskSignature{
		Name: "multiply",
		Args: []signatures.TaskArg{
			{
				Type:  "int64",
				Value: 4,
			},
		},
	}

	task4 := signatures.TaskSignature{
		Name: "multiply",
	}

	return []signatures.TaskSignature{
		task0, task1, task2, task3, task4,
	}
}

func _testSendTask(server *machinery.Server, t *testing.T) {
	tasks := _getTasks()

	asyncResult, err := server.SendTask(&tasks[0])
	if err != nil {
		t.Error(err)
	}

	result, err := asyncResult.Get()
	if err != nil {
		t.Error(err)
	}

	if result.Interface() != int64(2) {
		t.Errorf(
			"result = %v(%v), want int64(2)",
			result.Type().String(),
			result.Interface(),
		)
	}
}

func _testSendGroup(server *machinery.Server, t *testing.T) {
	tasks := _getTasks()

	group := machinery.NewGroup(&tasks[0], &tasks[1], &tasks[2])
	asyncResults, err := server.SendGroup(group)
	if err != nil {
		t.Error(err)
	}

	expectedResults := []int64{2, 4, 11}

	actualResults := make([]int64, 3)

	for i, asyncResult := range asyncResults {
		result, err := asyncResult.Get()
		if err != nil {
			t.Error(err)
		}
		intResult, ok := result.Interface().(int64)
		if !ok {
			t.Errorf("Could not convert %v to int64", result.Interface())
		}
		actualResults[i] = intResult
	}

	sort.Sort(ascendingInt64s(actualResults))

	if !reflect.DeepEqual(expectedResults, actualResults) {
		t.Errorf(
			"expected results = %v, actual results = %v",
			expectedResults,
			actualResults,
		)
	}
}

func _testSendChord(server *machinery.Server, t *testing.T) {
	tasks := _getTasks()

	group := machinery.NewGroup(&tasks[0], &tasks[1], &tasks[2])
	chord := machinery.NewChord(group, &tasks[4])
	chordAsyncResult, err := server.SendChord(chord)
	if err != nil {
		t.Error(err)
	}

	result, err := chordAsyncResult.Get()
	if err != nil {
		t.Error(err)
	}

	if result.Interface() != int64(88) {
		t.Errorf(
			"result = %v(%v), want int64(88)",
			result.Type().String(),
			result.Interface(),
		)
	}
}

func _testSendChain(server *machinery.Server, t *testing.T) {
	tasks := _getTasks()

	chain := machinery.NewChain(&tasks[1], &tasks[2], &tasks[3])
	chainAsyncResult, err := server.SendChain(chain)
	if err != nil {
		t.Error(err)
	}

	result, err := chainAsyncResult.Get()
	if err != nil {
		t.Error(err)
	}

	if result.Interface() != int64(60) {
		t.Errorf(
			"result = %v(%v), want int64(60)",
			result.Type().String(),
			result.Interface(),
		)
	}
}

func _setup(brokerURL, backend string) *machinery.Server {
	cnf := config.Config{
		Broker:        brokerURL,
		ResultBackend: backend,
		Exchange:      "test_exchange",
		ExchangeType:  "direct",
		DefaultQueue:  "test_queue",
		BindingKey:    "test_task",
	}

	server, err := machinery.NewServer(&cnf)
	errors.Fail(err, "Could not initialize server")

	tasks := map[string]interface{}{
		"add": func(args ...int64) (int64, error) {
			sum := int64(0)
			for _, arg := range args {
				sum += arg
			}
			return sum, nil
		},
		"multiply": func(args ...int64) (int64, error) {
			sum := int64(1)
			for _, arg := range args {
				sum *= arg
			}
			return sum, nil
		},
	}
	server.RegisterTasks(tasks)

	return server
}
