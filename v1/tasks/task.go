package tasks

import (
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"

	"context"

	"github.com/RichardKnop/machinery/v1/log"
)

// ErrTaskPanicked ...
var ErrTaskPanicked = errors.New("Invoking task caused a panic")

// Task wraps a signature and methods used to reflect task arguments and
// return values after invoking the task
type Task struct {
	TaskFunc   reflect.Value
	UseContext bool
	Args       []reflect.Value
}

// New tries to use reflection to convert the function and arguments
// into a reflect.Value and prepare it for invocation
func New(taskFunc interface{}, args []Arg) (*Task, error) {
	task := &Task{
		TaskFunc: reflect.ValueOf(taskFunc),
	}

	taskFuncType := reflect.TypeOf(taskFunc)
	if taskFuncType.NumIn() > 0 {
		arg0Type := taskFuncType.In(0)
		if IsContextType(arg0Type) {
			task.UseContext = true
		}
	}

	if err := task.ReflectArgs(args); err != nil {
		return nil, fmt.Errorf("Reflect task args error: %s", err)
	}

	return task, nil
}

// Call attempts to call the task with the supplied arguments.
//
// `err` is set in the return value in two cases:
// 1. The reflected function invocation panics (e.g. due to a mismatched
//    argument list).
// 2. The task func itself returns a non-nil error.
func (t *Task) Call() (taskResults []*TaskResult, err error) {
	defer func() {
		// Recover from panic and set err.
		if e := recover(); e != nil {
			switch e := e.(type) {
			default:
				err = ErrTaskPanicked
			case error:
				err = e
			case string:
				err = errors.New(e)
			}
			// Print stack trace
			log.ERROR.Printf("%s", debug.Stack())
		}
	}()

	args := t.Args

	if t.UseContext {
		ctx := context.Background()
		ctxValue := reflect.ValueOf(ctx)
		args = append([]reflect.Value{ctxValue}, args...)
	}

	// Invoke the task
	results := t.TaskFunc.Call(args)

	// Task must return at least a value
	if len(results) == 0 {
		return nil, ErrTaskReturnsNoValue
	}

	// Last returned value
	lastResult := results[len(results)-1]

	// If the last returned value is not nil, it has to be of error type, if that
	// is not the case, return error message, otherwise propagate the task error
	// to the caller
	if !lastResult.IsNil() {
		errorInterface := reflect.TypeOf((*error)(nil)).Elem()
		if !lastResult.Type().Implements(errorInterface) {
			return nil, ErrLastReturnValueMustBeError
		}

		return nil, lastResult.Interface().(error)
	}

	// Convert reflect values to task results
	taskResults = make([]*TaskResult, len(results)-1)
	for i := 0; i < len(results)-1; i++ {
		taskResults[i] = &TaskResult{
			Type:  reflect.TypeOf(results[i].Interface()).String(),
			Value: results[i].Interface(),
		}
	}

	return taskResults, err
}

// ReflectArgs converts []TaskArg to []reflect.Value
func (t *Task) ReflectArgs(args []Arg) error {
	argValues := make([]reflect.Value, len(args))

	for i, arg := range args {
		argValue, err := ReflectValue(arg.Type, arg.Value)
		if err != nil {
			return err
		}
		argValues[i] = argValue
	}

	t.Args = argValues
	return nil
}
