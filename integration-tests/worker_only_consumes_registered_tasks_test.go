package integration_test

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
)

func TestWorkerOnlyConsumesRegisteredTaskAMQP(t *testing.T) {
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		t.Skip("AMQP_URL is not defined")
	}

	cnf := config.Config{
		Broker:        amqpURL,
		DefaultQueue:  "test_queue",
		ResultBackend: amqpURL,
		Lock:          "eager",
		AMQP: &config.AMQPConfig{
			Exchange:      "test_exchange",
			ExchangeType:  "direct",
			BindingKey:    "test_task",
			PrefetchCount: 3,
		},
	}

	server1, err := machinery.NewServer(&cnf)
	if err != nil {
		t.Fatal(err, "Could not initialize server")
	}

	server1.RegisterTask("add", func(args ...int64) (int64, error) {
		sum := int64(0)
		for _, arg := range args {
			sum += arg
		}
		return sum, nil
	})

	server2, err := machinery.NewServer(&cnf)
	if err != nil {
		t.Fatal(err, "Could not initialize server")
	}

	server2.RegisterTask("multiply", func(args ...int64) (int64, error) {
		sum := int64(1)
		for _, arg := range args {
			sum *= arg
		}
		return sum, nil
	})

	task1 := tasks.Signature{
		Name: "add",
		Args: []tasks.Arg{
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

	task2 := tasks.Signature{
		Name: "multiply",
		Args: []tasks.Arg{
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

	worker1 := server1.NewWorker("test_worker", 0)
	worker2 := server2.NewWorker("test_worker2", 0)
	go worker1.Launch()
	go worker2.Launch()

	group, err := tasks.NewGroup(&task2, &task1)
	if err != nil {
		t.Fatal(err)
	}

	asyncResults, err := server1.SendGroup(group, 10)
	if err != nil {
		t.Error(err)
	}

	expectedResults := []int64{5, 20}
	actualResults := make([]int64, 2)

	for i, asyncResult := range asyncResults {
		results, err := asyncResult.Get(time.Duration(time.Millisecond * 5))
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

func TestWorkerOnlyConsumesRegisteredTaskRedis(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	cnf := config.Config{
		Broker:        fmt.Sprintf("redis://%v", redisURL),
		DefaultQueue:  "test_queue",
		ResultBackend: fmt.Sprintf("redis://%v", redisURL),
		Lock:          fmt.Sprintf("redis://%v", redisURL),
	}

	server1, err := machinery.NewServer(&cnf)
	if err != nil {
		t.Fatal(err)
	}

	server1.RegisterTask("add", func(args ...int64) (int64, error) {
		sum := int64(0)
		for _, arg := range args {
			sum += arg
		}
		return sum, nil
	})

	server2, err := machinery.NewServer(&cnf)
	if err != nil {
		t.Fatal(err)
	}

	server2.RegisterTask("multiply", func(args ...int64) (int64, error) {
		sum := int64(1)
		for _, arg := range args {
			sum *= arg
		}
		return sum, nil
	})

	task1 := tasks.Signature{
		Name: "add",
		Args: []tasks.Arg{
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

	task2 := tasks.Signature{
		Name: "multiply",
		Args: []tasks.Arg{
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

	worker1 := server1.NewWorker("test_worker", 0)
	worker2 := server2.NewWorker("test_worker2", 0)
	go worker1.Launch()
	go worker2.Launch()

	group, err := tasks.NewGroup(&task2, &task1)
	if err != nil {
		t.Fatal(err)
	}

	asyncResults, err := server1.SendGroup(group, 10)
	if err != nil {
		t.Error(err)
	}

	expectedResults := []int64{5, 20}
	actualResults := make([]int64, 2)

	for i, asyncResult := range asyncResults {
		results, err := asyncResult.Get(time.Duration(time.Millisecond * 5))
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
