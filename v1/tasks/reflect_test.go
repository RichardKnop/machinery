package tasks_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/tasks"
)

func TestReflectValue(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		value    interface{}
		expected string
	}{
		{
			name:     "bool",
			value:    false,
			expected: "bool",
		},
		{
			name:     "int",
			value:    float64(1),
			expected: "int",
		},
		{
			name:     "int8",
			value:    float64(1),
			expected: "int8",
		},
		{
			name:     "int16",
			value:    float64(1),
			expected: "int16",
		},
		{
			name:     "int32",
			value:    float64(1),
			expected: "int32",
		},
		{
			name:     "int64",
			value:    float64(1),
			expected: "int64",
		},
		{
			name:     "uint",
			value:    float64(1),
			expected: "uint",
		},
		{
			name:     "uint8",
			value:    float64(1),
			expected: "uint8",
		},
		{
			name:     "uint16",
			value:    float64(1),
			expected: "uint16",
		},
		{
			name:     "uint32",
			value:    float64(1),
			expected: "uint32",
		},
		{
			name:     "uint64",
			value:    float64(1),
			expected: "uint64",
		},
		{
			name:     "float32",
			value:    float64(0.5),
			expected: "float32",
		},
		{
			name:     "float64",
			value:    float64(0.5),
			expected: "float64",
		},
		{
			name:     "string",
			value:    "123",
			expected: "string",
		},
		{
			name:     "[]bool",
			value:    []interface{}{false, true},
			expected: "[]bool",
		},
		{
			name:     "[]int",
			value:    []interface{}{float64(1), float64(2)},
			expected: "[]int",
		},
		{
			name:     "[]int8",
			value:    []interface{}{float64(1), float64(2)},
			expected: "[]int8",
		},
		{
			name:     "[]int16",
			value:    []interface{}{float64(1), float64(2)},
			expected: "[]int16",
		},
		{
			name:     "[]int32",
			value:    []interface{}{float64(1), float64(2)},
			expected: "[]int32",
		},
		{
			name:     "[]int64",
			value:    []interface{}{float64(1), float64(2)},
			expected: "[]int64",
		},
		{
			name:     "[]uint",
			value:    []interface{}{float64(1), float64(2)},
			expected: "[]uint",
		},
		{
			name:     "[]uint8",
			value:    []interface{}{float64(1), float64(2)},
			expected: "[]uint8",
		},
		{
			name:     "[]uint16",
			value:    []interface{}{float64(1), float64(2)},
			expected: "[]uint16",
		},
		{
			name:     "[]uint32",
			value:    []interface{}{float64(1), float64(2)},
			expected: "[]uint32",
		},
		{
			name:     "[]uint64",
			value:    []interface{}{float64(1), float64(2)},
			expected: "[]uint64",
		},
		{
			name:     "[]float32",
			value:    []interface{}{float64(0.5), float64(1)},
			expected: "[]float32",
		},
		{
			name:     "[]float64",
			value:    []interface{}{float64(1), float64(0.5)},
			expected: "[]float64",
		},
		{
			name:     "[]string",
			value:    []interface{}{"foo", "bar"},
			expected: "[]string",
		},
	}

	for _, testCase := range testCases {
		value, err := tasks.ReflectValue(testCase.name, testCase.value)
		if err != nil {
			t.Error(err)
		}
		if value.Type().String() != testCase.expected {
			t.Errorf("type is %v, want %s", value.Type().String(), testCase.expected)
		}
	}
}
