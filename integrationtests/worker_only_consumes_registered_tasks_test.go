package integrationtests

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/errors"
	"github.com/RichardKnop/machinery/v1/signatures"
)

func TestWorkerOnlyConsumesRegisteredTaskAMQP(t *testing.T) {
	amqpURL := os.Getenv("AMQP_URL")

	if amqpURL != "" {
		cnf := config.Config{
			Broker:        amqpURL,
			ResultBackend: amqpURL,
			Exchange:      "test_exchange",
			ExchangeType:  "direct",
			DefaultQueue:  "test_queue",
			BindingKey:    "test_task",
		}

		server1, err := machinery.NewServer(&cnf)
		errors.Fail(err, "Could not initialize server")

		server1.RegisterTask("add", func(args ...int64) (int64, error) {
			sum := int64(0)
			for _, arg := range args {
				sum += arg
			}
			return sum, nil
		})

		server2, err := machinery.NewServer(&cnf)
		errors.Fail(err, "Could not initialize server")

		server2.RegisterTask("multiply", func(args ...int64) (int64, error) {
			sum := int64(1)
			for _, arg := range args {
				sum *= arg
			}
			return sum, nil
		})

		task1 := signatures.TaskSignature{
			Name: "add",
			Args: []signatures.TaskArg{
				{
					Type:  "int64",
					Value: 2,
				},
				{
					Type:  "int64",
					Value: 3,
				},
			},
		}

		task2 := signatures.TaskSignature{
			Name: "multiply",
			Args: []signatures.TaskArg{
				{
					Type:  "int64",
					Value: 4,
				},
				{
					Type:  "int64",
					Value: 5,
				},
			},
		}

		worker1 := server1.NewWorker("test_worker")
		worker2 := server2.NewWorker("test_worker2")
		go worker1.Launch()
		go worker2.Launch()

		group := machinery.NewGroup(&task2, &task1)
		asyncResults, err := server1.SendGroup(group)
		if err != nil {
			t.Error(err)
		}

		expectedResults := []int64{5, 20}
		actualResults := make([]int64, 2)

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

		worker1.Quit()
		worker2.Quit()

		sort.Sort(ascendingInt64s(actualResults))

		if !reflect.DeepEqual(expectedResults, actualResults) {
			t.Errorf(
				"expected results = %v, actual results = %v",
				expectedResults,
				actualResults,
			)
		}
	}
}

func TestWorkerOnlyConsumesRegisteredTaskRedis(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")

	if redisURL != "" {
		cnf := config.Config{
			Broker:        fmt.Sprintf("redis://%v", redisURL),
			ResultBackend: fmt.Sprintf("redis://%v", redisURL),
			Exchange:      "test_exchange",
			ExchangeType:  "direct",
			DefaultQueue:  "test_queue",
			BindingKey:    "test_task",
		}

		server1, err := machinery.NewServer(&cnf)
		errors.Fail(err, "Could not initialize server")

		server1.RegisterTask("add", func(args ...int64) (int64, error) {
			sum := int64(0)
			for _, arg := range args {
				sum += arg
			}
			return sum, nil
		})

		server2, err := machinery.NewServer(&cnf)
		errors.Fail(err, "Could not initialize server")

		server2.RegisterTask("multiply", func(args ...int64) (int64, error) {
			sum := int64(1)
			for _, arg := range args {
				sum *= arg
			}
			return sum, nil
		})

		task1 := signatures.TaskSignature{
			Name: "add",
			Args: []signatures.TaskArg{
				{
					Type:  "int64",
					Value: 2,
				},
				{
					Type:  "int64",
					Value: 3,
				},
			},
		}

		task2 := signatures.TaskSignature{
			Name: "multiply",
			Args: []signatures.TaskArg{
				{
					Type:  "int64",
					Value: 4,
				},
				{
					Type:  "int64",
					Value: 5,
				},
			},
		}

		worker1 := server1.NewWorker("test_worker")
		worker2 := server2.NewWorker("test_worker2")
		go worker1.Launch()
		go worker2.Launch()

		group := machinery.NewGroup(&task2, &task1)
		asyncResults, err := server1.SendGroup(group)
		if err != nil {
			t.Error(err)
		}

		expectedResults := []int64{5, 20}
		actualResults := make([]int64, 2)

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

		worker1.Quit()
		worker2.Quit()

		sort.Sort(ascendingInt64s(actualResults))

		if !reflect.DeepEqual(expectedResults, actualResults) {
			t.Errorf(
				"expected results = %v, actual results = %v",
				expectedResults,
				actualResults,
			)
		}
	}
}
