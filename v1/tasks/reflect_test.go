package tasks_test

import (
	"encoding/json"
	"testing"

	"github.com/RichardKnop/machinery/v1/tasks"
)

func TestReflectValue(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		value         interface{}
		expectedType  string
		expectedValue interface{}
	}{
		{
			name:         "bool",
			value:        false,
			expectedType: "bool",
		},
		{
			name:          "int",
			value:         json.Number("123"),
			expectedType:  "int",
			expectedValue: int(123),
		},
		{
			name:          "int8",
			value:         json.Number("123"),
			expectedType:  "int8",
			expectedValue: int8(123),
		},
		{
			name:          "int16",
			value:         json.Number("123"),
			expectedType:  "int16",
			expectedValue: int16(123),
		},
		{
			name:          "int32",
			value:         json.Number("123"),
			expectedType:  "int32",
			expectedValue: int32(123),
		},
		{
			name:          "int64",
			value:         json.Number("185135722552891243"),
			expectedType:  "int64",
			expectedValue: int64(185135722552891243),
		},
		{
			name:          "uint",
			value:         json.Number("123"),
			expectedType:  "uint",
			expectedValue: uint(123),
		},
		{
			name:          "uint8",
			value:         json.Number("123"),
			expectedType:  "uint8",
			expectedValue: uint8(123),
		},
		{
			name:          "uint16",
			value:         json.Number("123"),
			expectedType:  "uint16",
			expectedValue: uint16(123),
		},
		{
			name:          "uint32",
			value:         json.Number("123"),
			expectedType:  "uint32",
			expectedValue: uint32(123),
		},
		{
			name:          "uint64",
			value:         json.Number("185135722552891243"),
			expectedType:  "uint64",
			expectedValue: uint64(185135722552891243),
		},
		{
			name:          "float32",
			value:         json.Number("0.5"),
			expectedType:  "float32",
			expectedValue: float32(0.5),
		},
		{
			name:          "float64",
			value:         json.Number("0.5"),
			expectedType:  "float64",
			expectedValue: float64(0.5),
		},
		{
			name:          "string",
			value:         "123",
			expectedType:  "string",
			expectedValue: "123",
		},
		{
			name:         "[]bool",
			value:        []interface{}{false, true},
			expectedType: "[]bool",
		},
		{
			name:         "[]int",
			value:        []interface{}{json.Number("123"), json.Number("234")},
			expectedType: "[]int",
		},
		{
			name:         "[]int8",
			value:        []interface{}{json.Number("123"), json.Number("234")},
			expectedType: "[]int8",
		},
		{
			name:         "[]int16",
			value:        []interface{}{json.Number("123"), json.Number("234")},
			expectedType: "[]int16",
		},
		{
			name:         "[]int32",
			value:        []interface{}{json.Number("123"), json.Number("234")},
			expectedType: "[]int32",
		},
		{
			name:         "[]int64",
			value:        []interface{}{json.Number("123"), json.Number("234")},
			expectedType: "[]int64",
		},
		{
			name:         "[]uint",
			value:        []interface{}{json.Number("123"), json.Number("234")},
			expectedType: "[]uint",
		},
		{
			name:         "[]uint8",
			value:        []interface{}{json.Number("123"), json.Number("234")},
			expectedType: "[]uint8",
		},
		{
			name:         "[]uint16",
			value:        []interface{}{json.Number("123"), json.Number("234")},
			expectedType: "[]uint16",
		},
		{
			name:         "[]uint32",
			value:        []interface{}{json.Number("123"), json.Number("234")},
			expectedType: "[]uint32",
		},
		{
			name:         "[]uint64",
			value:        []interface{}{json.Number("123"), json.Number("234")},
			expectedType: "[]uint64",
		},
		{
			name:         "[]float32",
			value:        []interface{}{json.Number("0.5"), json.Number("1.28")},
			expectedType: "[]float32",
		},
		{
			name:         "[]float64",
			value:        []interface{}{json.Number("0.5"), json.Number("1.28")},
			expectedType: "[]float64",
		},
		{
			name:         "[]string",
			value:        []interface{}{"foo", "bar"},
			expectedType: "[]string",
		},
	}

	for _, testCase := range testCases {
		value, err := tasks.ReflectValue(testCase.name, testCase.value)
		if err != nil {
			t.Error(err)
		}
		if value.Type().String() != testCase.expectedType {
			t.Errorf("type is %v, want %s", value.Type().String(), testCase.expectedType)
		}
		if testCase.expectedValue != nil {
			if value.Interface() != testCase.expectedValue {
				t.Errorf("value is %v, want %v", value.Interface(), testCase.expectedValue)
			}
		}
	}
}
