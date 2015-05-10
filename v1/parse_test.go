package machinery

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func TestParseNumberArgs(t *testing.T) {
	var args []interface{}
	_ = json.Unmarshal([]byte(`[1, 2.5, 4.68]`), &args)

	parsedArgs, err := ParseNumberArgs(args)

	if err != nil {
		t.Errorf(err.Error())
	}

	if !reflect.DeepEqual(parsedArgs, []float64{1.0, 2.5, 4.68}) {
		t.Errorf(
			"parsedArgs = %v, should be []float64{1.0, 2.5, 4.68}",
			parsedArgs,
		)
	}
}

func TestParseNumberArgsError(t *testing.T) {
	var args []interface{}
	_ = json.Unmarshal([]byte(`[1, "what", 4.68]`), &args)

	parsedArgs, err := ParseNumberArgs(args)

	if parsedArgs != nil {
		t.Errorf("parsedArgs = %v, should be nil", parsedArgs)
	}

	expectedErr := errors.New(
		"2. arg is not a valid floating point number: what")
	if err.Error() != expectedErr.Error() {
		t.Errorf("err = %v, want %v", err, expectedErr)
	}
}
