package machinery_test

import (
	"math"
	"reflect"
	"testing"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/signatures"
	"github.com/stretchr/testify/assert"
)

func TestInvalidArgRobustness(t *testing.T) {
	// Create a func and reflect it
	fValue := reflect.ValueOf(func(x int) {})

	// Construct an invalid argument list and reflect it
	args := []signatures.TaskArg{
		{"bool", true},
	}

	argValues, err := machinery.ReflectArgs(args)
	if assert.NoError(t, err) {
		// Invoke TryCall and validate error handling
		results, err := machinery.TryCall(fValue, argValues)
		assert.Equal(t, "reflect: Call using bool as type int", err.Error())
		assert.Nil(t, results)
	}
}

func TestInterfaceValuedResult(t *testing.T) {
	f := func() interface{} { return math.Pi }
	value := reflect.ValueOf(f())
	taskResult := machinery.CreateTaskResult(value)
	assert.Equal(t, "float64", taskResult.Type)
	assert.Equal(t, math.Pi, taskResult.Value)
}
