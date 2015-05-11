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
	typeConversionError = func(argValue interface{}, argTypeStr string) error {
		return fmt.Errorf("%v is not %v", argValue, argTypeStr)
	}
)

// ReflectArgs converts []TaskArg to []reflect.Value
func ReflectArgs(args []TaskArg) ([]reflect.Value, error) {
	argValues := make([]reflect.Value, len(args))

	for i, arg := range args {
		argType := typesMap[arg.Type]
		argTypeStr := argType.String()
		argValue := reflect.New(argType)

		if strings.HasPrefix(argTypeStr, "int") {
			intValue, ok := arg.Value.(int64)
			if !ok {
				return nil, typeConversionError(arg.Value, argTypeStr)
			}
			argValue.Elem().SetInt(intValue)
			argValues[i] = argValue.Elem()
			continue
		}

		if argTypeStr == "uint" {
			uintValue, ok := arg.Value.(uint64)
			if !ok {
				return nil, typeConversionError(arg.Value, argTypeStr)
			}
			argValue.Elem().SetUint(uintValue)
			argValues[i] = argValue.Elem()
			continue
		}

		if strings.HasPrefix(argTypeStr, "float") {
			floatValue, ok := arg.Value.(float64)
			if !ok {
				return nil, typeConversionError(arg.Value, argTypeStr)
			}
			argValue.Elem().SetFloat(floatValue)
			argValues[i] = argValue.Elem()
			continue
		}

		if argTypeStr == "string" {
			stringValue, ok := arg.Value.(string)
			if !ok {
				return nil, typeConversionError(arg.Value, argTypeStr)
			}
			argValue.Elem().SetString(stringValue)
			argValues[i] = argValue.Elem()
			continue
		}

		return nil, fmt.Errorf("%v is not one of supported types", arg.Value)
	}

	return argValues, nil
}
