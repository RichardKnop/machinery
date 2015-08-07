package machinery

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/errors"
	"github.com/RichardKnop/machinery/v1/signatures"
)

var (
	task0, task1, task2, task3, task4 signatures.TaskSignature
)

type ascendingInt64s []int64

func (a ascendingInt64s) Len() int           { return len(a) }
func (a ascendingInt64s) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ascendingInt64s) Less(i, j int) bool { return a[i] < a[j] }

func init() {
	task0 = signatures.TaskSignature{
		Name: "add",
		Args: []signatures.TaskArg{
			signatures.TaskArg{
				Type:  "int64",
				Value: 1,
			},
			signatures.TaskArg{
				Type:  "int64",
				Value: 1,
			},
		},
	}

	task1 = signatures.TaskSignature{
		Name: "add",
		Args: []signatures.TaskArg{
			signatures.TaskArg{
				Type:  "int64",
				Value: 2,
			},
			signatures.TaskArg{
				Type:  "int64",
				Value: 2,
			},
		},
	}

	task2 = signatures.TaskSignature{
		Name: "add",
		Args: []signatures.TaskArg{
			signatures.TaskArg{
				Type:  "int64",
				Value: 5,
			},
			signatures.TaskArg{
				Type:  "int64",
				Value: 6,
			},
		},
	}

	task3 = signatures.TaskSignature{
		Name: "multiply",
		Args: []signatures.TaskArg{
			signatures.TaskArg{
				Type:  "int64",
				Value: 4,
			},
		},
	}

	task4 = signatures.TaskSignature{
		Name: "multiply",
	}
}

func TestIntegration(t *testing.T) {
	brokerURL := os.Getenv("AMQP_URL")
	if brokerURL == "" {
		return
	}
	//
	// server1 := setup(brokerURL, "amqp")
	// worker1 := server1.NewWorker("test_worker")
	// go worker1.Launch()
	// _testSendTask(server1, t)
	// _testSendGroup(server1, t)
	// _testSendChord(server1, t)
	// _testSendChain(server1, t)
	// worker1.Quit()

	memcacheURL := os.Getenv("MEMCACHE_URL")
	if memcacheURL == "" {
		return
	}

	server2 := setup(brokerURL, fmt.Sprintf("memcache://%v", memcacheURL))
	worker2 := server2.NewWorker("test_worker")
	go worker2.Launch()
	_testSendTask(server2, t)
	_testSendGroup(server2, t)
	_testSendChord(server2, t)
	_testSendChain(server2, t)
	worker2.Quit()
}

func _testSendTask(server *Server, t *testing.T) {
	asyncResult, err := server.SendTask(&task0)
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

func _testSendGroup(server *Server, t *testing.T) {
	group := NewGroup(&task0, &task1, &task2)
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

func _testSendChord(server *Server, t *testing.T) {
	group := NewGroup(&task0, &task1, &task2)
	chord := NewChord(group, &task4)
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

func _testSendChain(server *Server, t *testing.T) {
	chain := NewChain(&task1, &task2, &task3)
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

func setup(brokerURL, backend string) *Server {
	cnf := config.Config{
		Broker:        brokerURL,
		ResultBackend: backend,
		Exchange:      "test_exchange",
		ExchangeType:  "direct",
		DefaultQueue:  "test_queue",
		BindingKey:    "test_task",
	}

	server, err := NewServer(&cnf)
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
