package machinery

import (
	. "github.com/RichardKnop/machinery/v1/signatures"
	"reflect"
	"testing"
)

func TestInvalidArgRobustness(t *testing.T) {
	// Create a func and reflect it
	fValue := reflect.ValueOf(func(x int) {})

	// Construct an invalid argument list and reflect it
	args := []TaskArg{
		TaskArg{"bool", true},
	}

	argValues, err := reflectArgs(args)
	if err != nil {
		t.Errorf("reflectArgs error = %v, want nil", err)
	}

	// Invoke tryCall and validate error handling
	results, err := tryCall(fValue, argValues)

	expectedMessage := "reflect: Call using bool as type int"
	if err.Error() != expectedMessage {
		t.Errorf("tryCall error = %v, want %v", err, expectedMessage)
	}

	if results != nil {
		t.Errorf("results = %v, want nil", results)
	}
}
