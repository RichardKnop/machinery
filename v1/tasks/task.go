package tasks

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"

	"github.com/RichardKnop/machinery/v1/log"

	"go.opencensus.io/trace"
)

// ErrTaskPanicked ...
var ErrTaskPanicked = errors.New("Invoking task caused a panic")

// Task wraps a signature and methods used to reflect task arguments and
// return values after invoking the task
type Task struct {
	TaskFunc   reflect.Value
	UseContext bool
	Context    context.Context
	Args       []reflect.Value
}

// New tries to use reflection to convert the function and arguments
// into a reflect.Value and prepare it for invocation
func New(taskFunc interface{}, args []Arg) (*Task, error) {
	task := &Task{
		TaskFunc: reflect.ValueOf(taskFunc),
		Context:  context.Background(),
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
	// retrieve the span from the task's context and finish it as soon as this function returns
	if span := trace.FromContext(t.Context); span != nil {
		defer span.End()
	}

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

			// mark the span as failed and dump the error and stack trace to the span
			if span := trace.FromContext(t.Context); span != nil {
				span.AddAttributes(trace.BoolAttribute("error", true))
			}

			// Print stack trace
			log.ERROR.Printf("%s", debug.Stack())
		}
	}()

	args := t.Args

	if t.UseContext {
		ctxValue := reflect.ValueOf(t.Context)
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
		// If the result implements Retriable interface, return instance of Retriable
		retriableErrorInterface := reflect.TypeOf((*Retriable)(nil)).Elem()
		if lastResult.Type().Implements(retriableErrorInterface) {
			return nil, lastResult.Interface().(ErrRetryTaskLater)
		}

		// Otherwise, check that the result implements the standard error interface,
		// if not, return ErrLastReturnValueMustBeError error
		errorInterface := reflect.TypeOf((*error)(nil)).Elem()
		if !lastResult.Type().Implements(errorInterface) {
			return nil, ErrLastReturnValueMustBeError
		}

		// Return the standard error
		return nil, lastResult.Interface().(error)
	}

	// Convert reflect values to task results
	taskResults = make([]*TaskResult, len(results)-1)
	for i := 0; i < len(results)-1; i++ {
		val := results[i].Interface()
		typeStr := reflect.TypeOf(val).String()
		taskResults[i] = &TaskResult{
			Type:  typeStr,
			Value: val,
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
