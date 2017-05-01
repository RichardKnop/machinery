package integrationtests

import (
	"errors"
	"log"
	"reflect"
	"sort"
	"testing"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/stretchr/testify/assert"
)

type ascendingInt64s []int64

func (a ascendingInt64s) Len() int           { return len(a) }
func (a ascendingInt64s) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ascendingInt64s) Less(i, j int) bool { return a[i] < a[j] }

func _getTasks() []*signatures.TaskSignature {
	task0 := &signatures.TaskSignature{
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

	task1 := &signatures.TaskSignature{
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

	task2 := &signatures.TaskSignature{
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

	task3 := &signatures.TaskSignature{
		Name: "multiply",
		Args: []signatures.TaskArg{
			{
				Type:  "int64",
				Value: 4,
			},
		},
	}

	task4 := &signatures.TaskSignature{
		Name: "multiply",
	}

	task5 := &signatures.TaskSignature{
		Name: "return_just_error",
		Args: []signatures.TaskArg{
			{
				Type:  "string",
				Value: "Test error",
			},
		},
	}

	task6 := &signatures.TaskSignature{
		Name: "return_multiple_values",
		Args: []signatures.TaskArg{
			{
				Type:  "string",
				Value: "foo",
			},
			{
				Type:  "string",
				Value: "bar",
			},
		},
	}

	return []*signatures.TaskSignature{
		task0, task1, task2, task3, task4, task5, task6,
	}
}

func _testSendTask(server *machinery.Server, t *testing.T) {
	tasks := _getTasks()

	asyncResult, err := server.SendTask(tasks[0])
	if err != nil {
		t.Error(err)
	}

	results, err := asyncResult.Get()
	if err != nil {
		t.Error(err)
	}

	if len(results) != 1 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 1)
	}

	if results[0].Interface() != int64(2) {
		t.Errorf(
			"result = %v(%v), want int64(2)",
			results[0].Type().String(),
			results[0].Interface(),
		)
	}
}

func _testSendGroup(server *machinery.Server, t *testing.T) {
	tasks := _getTasks()

	group := machinery.NewGroup(tasks[0], tasks[1], tasks[2])
	asyncResults, err := server.SendGroup(group)
	if err != nil {
		t.Error(err)
	}

	expectedResults := []int64{2, 4, 11}

	actualResults := make([]int64, 3)

	for i, asyncResult := range asyncResults {
		results, err := asyncResult.Get()
		if err != nil {
			t.Error(err)
		}

		if len(results) != 1 {
			t.Errorf("Number of results returned = %d. Wanted %d", len(results), 1)
		}

		intResult, ok := results[0].Interface().(int64)
		if !ok {
			t.Errorf("Could not convert %v to int64", results[0].Interface())
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

	group := machinery.NewGroup(tasks[0], tasks[1], tasks[2])
	chord := machinery.NewChord(group, tasks[4])
	chordAsyncResult, err := server.SendChord(chord)
	if err != nil {
		t.Error(err)
	}

	results, err := chordAsyncResult.Get()
	if err != nil {
		t.Error(err)
	}

	if len(results) != 1 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 1)
	}

	if results[0].Interface() != int64(88) {
		t.Errorf(
			"result = %v(%v), want int64(88)",
			results[0].Type().String(),
			results[0].Interface(),
		)
	}
}

func _testSendChain(server *machinery.Server, t *testing.T) {
	tasks := _getTasks()

	chain := machinery.NewChain(tasks[1], tasks[2], tasks[3])
	chainAsyncResult, err := server.SendChain(chain)
	if err != nil {
		t.Error(err)
	}

	results, err := chainAsyncResult.Get()
	if err != nil {
		t.Error(err)
	}

	if len(results) != 1 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 1)
	}

	if results[0].Interface() != int64(60) {
		t.Errorf(
			"result = %v(%v), want int64(60)",
			results[0].Type().String(),
			results[0].Interface(),
		)
	}
}

func _testReturnJustError(server *machinery.Server, t *testing.T) {
	tasks := _getTasks()

	asyncResult, err := server.SendTask(tasks[5])
	if err != nil {
		t.Error(err)
	}

	results, err := asyncResult.Get()

	if len(results) != 0 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 0)
	}

	assert.Equal(t, "Test error", err.Error())
}

func _testReturnMultipleValues(server *machinery.Server, t *testing.T) {
	tasks := _getTasks()

	asyncResult, err := server.SendTask(tasks[6])
	if err != nil {
		t.Error(err)
	}

	results, err := asyncResult.Get()
	if err != nil {
		t.Error(err)
	}

	if len(results) != 2 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 2)
	}

	if results[0].Interface() != "foo" {
		t.Errorf(
			"result = %v(%v), want string(\"foo\":)",
			results[0].Type().String(),
			results[0].Interface(),
		)
	}

	if results[1].Interface() != "bar" {
		t.Errorf(
			"result = %v(%v), want string(\"bar\":)",
			results[1].Type().String(),
			results[1].Interface(),
		)
	}
}

func _setup(brokerURL, backend string) *machinery.Server {
	cnf := config.Config{
		Broker:        brokerURL,
		DefaultQueue:  "test_queue",
		ResultBackend: backend,
		AMQP: &config.AMQPConfig{
			Exchange:      "test_exchange",
			ExchangeType:  "direct",
			BindingKey:    "test_task",
			PrefetchCount: 1,
		},
	}

	server, err := machinery.NewServer(&cnf)
	if err != nil {
		log.Fatal(err, "Could not initialize server")
	}

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
		"return_just_error": func(arg string) error {
			return errors.New(arg)
		},
		"return_multiple_values": func(arg1, arg2 string) (string, string, error) {
			return arg1, arg2, nil
		},
	}
	server.RegisterTasks(tasks)

	return server
}
