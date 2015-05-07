package machinery

import (
	"encoding/json"
	"reflect"
	"testing"
)

var testNumberArgs = []byte(`[1, 2.5, 4.68]`)
var testInvalidNumberArgs = []byte(`[1, "what", 4.68]`)

func TestParseNumberArgs(t *testing.T) {
	var args []interface{}
	_ = json.Unmarshal(testNumberArgs, &args)

	parsedArgs, err := ParseNumberArgs(args)

	if err != nil {
		t.Errorf(err.Error())
	}

	if !reflect.DeepEqual(parsedArgs, []float64{1.0, 2.5, 4.68}) {
		t.Errorf("parsedArgs = %v, should be []float64{1.0, 2.5, 4.68}", parsedArgs)
	}
}

func TestParseNumberArgsError(t *testing.T) {
	var args []interface{}
	_ = json.Unmarshal(testInvalidNumberArgs, &args)

	parsedArgs, err := ParseNumberArgs(args)

	if err == nil {
		t.Errorf("Should have failed but instead returned %v", parsedArgs)
	}
}
