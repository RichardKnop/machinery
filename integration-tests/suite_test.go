package integration_test

import (
	"errors"
	"log"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/stretchr/testify/assert"
)

type ascendingInt64s []int64

func (a ascendingInt64s) Len() int           { return len(a) }
func (a ascendingInt64s) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ascendingInt64s) Less(i, j int) bool { return a[i] < a[j] }

func testAll(server *machinery.Server, t *testing.T) {
	testSendTask(server, t)
	testSendGroup(server, t, 0) // with unlimited concurrency
	testSendGroup(server, t, 2) // with limited concurrency (2 parallel tasks at the most)
	testSendChord(server, t)
	testSendChain(server, t)
	testReturnJustError(server, t)
	testReturnMultipleValues(server, t)
	testPanic(server, t)
	testDelay(server, t)
}

func testSendTask(server *machinery.Server, t *testing.T) {
	addTask := newAddTask(1, 1)

	asyncResult, err := server.SendTask(addTask)
	if err != nil {
		t.Error(err)
	}

	results, err := asyncResult.Get(time.Duration(time.Millisecond * 5))
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

	sumTask := newSumTask([]int64{1, 2})
	asyncResult, err = server.SendTask(sumTask)
	if err != nil {
		t.Error(err)
	}

	results, err = asyncResult.Get(time.Duration(time.Millisecond * 5))
	if err != nil {
		t.Error(err)
	}

	if len(results) != 1 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 1)
	}

	if results[0].Interface() != int64(3) {
		t.Errorf(
			"result = %v(%v), want int64(3)",
			results[0].Type().String(),
			results[0].Interface(),
		)
	}
}

func testSendGroup(server *machinery.Server, t *testing.T, sendConcurrency int) {
	t1, t2, t3 := newAddTask(1, 1), newAddTask(2, 2), newAddTask(5, 6)

	group, err := tasks.NewGroup(t1, t2, t3)
	if err != nil {
		t.Fatal(err)
	}

	asyncResults, err := server.SendGroup(group, sendConcurrency)
	if err != nil {
		t.Error(err)
	}

	expectedResults := []int64{2, 4, 11}

	actualResults := make([]int64, 3)

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

	sort.Sort(ascendingInt64s(actualResults))

	if !reflect.DeepEqual(expectedResults, actualResults) {
		t.Errorf(
			"expected results = %v, actual results = %v",
			expectedResults,
			actualResults,
		)
	}
}

func testSendChain(server *machinery.Server, t *testing.T) {
	t1, t2, t3 := newAddTask(2, 2), newAddTask(5, 6), newMultipleTask(4)

	chain, err := tasks.NewChain(t1, t2, t3)
	if err != nil {
		t.Fatal(err)
	}

	chainAsyncResult, err := server.SendChain(chain)
	if err != nil {
		t.Error(err)
	}

	results, err := chainAsyncResult.Get(time.Duration(time.Millisecond * 5))
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

func testSendChord(server *machinery.Server, t *testing.T) {
	t1, t2, t3, t4 := newAddTask(1, 1), newAddTask(2, 2), newAddTask(5, 6), newMultipleTask()

	group, err := tasks.NewGroup(t1, t2, t3)
	if err != nil {
		t.Fatal(err)
	}

	chord, err := tasks.NewChord(group, t4)
	if err != nil {
		t.Fatal(err)
	}

	chordAsyncResult, err := server.SendChord(chord, 10)
	if err != nil {
		t.Error(err)
	}

	results, err := chordAsyncResult.Get(time.Duration(time.Millisecond * 5))
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

func testReturnJustError(server *machinery.Server, t *testing.T) {
	// Fails, returns error as the only value
	task := newErrorTask("Test error", true)
	asyncResult, err := server.SendTask(task)
	if err != nil {
		t.Error(err)
	}

	results, err := asyncResult.Get(time.Duration(time.Millisecond * 5))
	if len(results) != 0 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 0)
	}
	assert.Equal(t, "Test error", err.Error())

	// Successful, returns nil as the only value
	task = newErrorTask("", false)
	asyncResult, err = server.SendTask(task)
	if err != nil {
		t.Error(err)
	}

	results, err = asyncResult.Get(time.Duration(time.Millisecond * 5))
	if len(results) != 0 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 0)
	}
	assert.NoError(t, err)
}

func testReturnMultipleValues(server *machinery.Server, t *testing.T) {
	// Successful task with multiple return values
	task := newMultipleReturnTask("foo", "bar", false)

	asyncResult, err := server.SendTask(task)
	if err != nil {
		t.Error(err)
	}

	results, err := asyncResult.Get(time.Duration(time.Millisecond * 5))
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

	// Failed task with multiple return values
	task = newMultipleReturnTask("", "", true)

	asyncResult, err = server.SendTask(task)
	if err != nil {
		t.Error(err)
	}

	results, err = asyncResult.Get(time.Duration(time.Millisecond * 5))
	if len(results) != 0 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 0)
	}
	assert.Error(t, err)
}

func testPanic(server *machinery.Server, t *testing.T) {
	task := &tasks.Signature{Name: "panic"}
	asyncResult, err := server.SendTask(task)
	if err != nil {
		t.Error(err)
	}

	results, err := asyncResult.Get(time.Duration(time.Millisecond * 5))
	if len(results) != 0 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 0)
	}
	assert.Equal(t, "oops", err.Error())
}

func testDelay(server *machinery.Server, t *testing.T) {
	now := time.Now().UTC()
	eta := now.Add(100 * time.Millisecond)
	task := newDelayTask(eta)
	asyncResult, err := server.SendTask(task)
	if err != nil {
		t.Error(err)
	}

	results, err := asyncResult.Get(time.Duration(5 * time.Millisecond))
	if err != nil {
		t.Error(err)
	}

	if len(results) != 1 {
		t.Errorf("Number of results returned = %d. Wanted %d", len(results), 1)
	}

	tm, ok := results[0].Interface().(int64)
	if !ok {
		t.Errorf(
			"Could not type assert = %v(%v) to int64",
			results[0].Type().String(),
			results[0].Interface(),
		)
	}

	if tm < eta.UnixNano() {
		t.Errorf(
			"result = %v(%v), want >= int64(%d)",
			results[0].Type().String(),
			results[0].Interface(),
			eta.UnixNano(),
		)
	}
}

func testSetup(cnf *config.Config) *machinery.Server {
	server, err := machinery.NewServer(cnf)
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
		"sum": func(numbers []int64) (int64, error) {
			var sum int64
			for _, num := range numbers {
				sum += num
			}
			return sum, nil
		},
		"return_just_error": func(msg string, fail bool) (err error) {
			if fail {
				err = errors.New(msg)
			}
			return err
		},
		"return_multiple_values": func(arg1, arg2 string, fail bool) (r1 string, r2 string, err error) {
			if fail {
				err = errors.New("some error")
			} else {
				r1 = arg1
				r2 = arg2
			}
			return r1, r2, err
		},
		"panic": func() (string, error) {
			panic(errors.New("oops"))
		},
		"delay_test": func() (int64, error) {
			return time.Now().UTC().UnixNano(), nil
		},
	}
	server.RegisterTasks(tasks)

	return server
}

func newAddTask(a, b int) *tasks.Signature {
	return &tasks.Signature{
		Name: "add",
		Args: []tasks.Arg{
			{
				Type:  "int64",
				Value: a,
			},
			{
				Type:  "int64",
				Value: b,
			},
		},
	}
}

func newMultipleTask(nums ...int) *tasks.Signature {
	args := make([]tasks.Arg, len(nums))
	for i, n := range nums {
		args[i] = tasks.Arg{
			Type:  "int64",
			Value: n,
		}
	}
	return &tasks.Signature{
		Name: "multiply",
		Args: args,
	}
}

func newSumTask(nums []int64) *tasks.Signature {
	return &tasks.Signature{
		Name: "sum",
		Args: []tasks.Arg{
			{
				Type:  "[]int64",
				Value: nums,
			},
		},
	}
}

func newErrorTask(msg string, fail bool) *tasks.Signature {
	return &tasks.Signature{
		Name: "return_just_error",
		Args: []tasks.Arg{
			{
				Type:  "string",
				Value: msg,
			},
			{
				Type:  "bool",
				Value: fail,
			},
		},
	}
}

func newMultipleReturnTask(arg1, arg2 string, fail bool) *tasks.Signature {
	return &tasks.Signature{
		Name: "return_multiple_values",
		Args: []tasks.Arg{
			{
				Type:  "string",
				Value: arg1,
			},
			{
				Type:  "string",
				Value: arg2,
			},
			{
				Type:  "bool",
				Value: fail,
			},
		},
	}
}

func newDelayTask(eta time.Time) *tasks.Signature {
	return &tasks.Signature{
		Name: "delay_test",
		ETA:  &eta,
	}
}
