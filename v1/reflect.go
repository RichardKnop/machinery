package machinery

import (
	"fmt"
	"reflect"
	"strings"
)

var (
	typesMap = map[string]reflect.Type{
		"int":     reflect.TypeOf(int(1)),
		"int8":    reflect.TypeOf(int8(1)),
		"int16":   reflect.TypeOf(int16(1)),
		"int32":   reflect.TypeOf(int32(1)),
		"int64":   reflect.TypeOf(int64(1)),
		"uint":    reflect.TypeOf(uint(1)),
		"float32": reflect.TypeOf(float32(0.5)),
		"float64": reflect.TypeOf(float64(0.5)),
		"string":  reflect.TypeOf(string("")),
	}
)

// ReflectArgs converts []TaskArg to []reflect.Value
func ReflectArgs(args []TaskArg) ([]reflect.Value, error) {
	argValues := make([]reflect.Value, len(args))

	for i, arg := range args {
		argType := typesMap[arg.Type]
		argTypeString := argType.String()
		argValue := reflect.New(argType)

		if strings.HasPrefix(argTypeString, "int") {
			intValue, ok := arg.Value.(int64)
			if !ok {
				return nil, fmt.Errorf("%v is not %v", arg.Value, argTypeString)
			}
			argValue.Elem().SetInt(intValue)
		}

		if argTypeString == "uint" {
			uintValue, ok := arg.Value.(uint64)
			if !ok {
				return nil, fmt.Errorf("%v is not %v", arg.Value, argTypeString)
			}
			argValue.Elem().SetUint(uintValue)
		}

		if strings.HasPrefix(argTypeString, "float") {
			floatValue, ok := arg.Value.(float64)
			if !ok {
				return nil, fmt.Errorf("%v is not %v", arg.Value, argTypeString)
			}
			argValue.Elem().SetFloat(floatValue)
		}

		if argTypeString == "string" {
			stringValue, ok := arg.Value.(string)
			if !ok {
				return nil, fmt.Errorf("%v is not %v", arg.Value, argTypeString)
			}
			argValue.Elem().SetString(stringValue)
		}

		argValues[i] = argValue.Elem()
	}

	return argValues, nil
}
