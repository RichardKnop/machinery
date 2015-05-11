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
		"uint8":   reflect.TypeOf(uint8(1)),
		"uint16":  reflect.TypeOf(uint16(1)),
		"uint32":  reflect.TypeOf(uint32(1)),
		"uint64":  reflect.TypeOf(uint64(1)),
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
		argType := typesMap[arg.Type].String()
		argValue := reflect.New(typesMap[arg.Type])

		// Integers
		if strings.HasPrefix(argType, "int") {
			intValue, err := getIntValue(&arg)
			if err != nil {
				return nil, err
			}

			argValue.Elem().SetInt(intValue)
			argValues[i] = argValue.Elem()
			continue
		}

		// Unbound integers
		if strings.HasPrefix(argType, "uint") {
			uintValue, err := getUintValue(&arg)
			if err != nil {
				return nil, err
			}

			argValue.Elem().SetUint(uintValue)
			argValues[i] = argValue.Elem()
			continue
		}

		// Floating point numbers
		if strings.HasPrefix(argType, "float") {
			floatValue, err := getFloatValue(&arg)
			if err != nil {
				return nil, err
			}

			argValue.Elem().SetFloat(floatValue)
			argValues[i] = argValue.Elem()
			continue
		}

		// Strings
		if argType == "string" {
			stringValue, ok := arg.Value.(string)
			if !ok {
				return nil, typeConversionError(arg.Value, argType)
			}

			argValue.Elem().SetString(stringValue)
			argValues[i] = argValue.Elem()
			continue
		}

		return nil, fmt.Errorf("%v is not one of supported types", arg.Value)
	}

	return argValues, nil
}

func getIntValue(arg *TaskArg) (int64, error) {
	argType := typesMap[arg.Type].String()

	switch argType {
	case "int":
		intValue, ok := arg.Value.(int)
		if ok {
			return int64(intValue), nil
		}
	case "int8":
		intValue, ok := arg.Value.(int8)
		if ok {
			return int64(intValue), nil
		}
	case "int16":
		intValue, ok := arg.Value.(int16)
		if ok {
			return int64(intValue), nil
		}
	case "int32":
		intValue, ok := arg.Value.(int32)
		if ok {
			return int64(intValue), nil
		}
	case "int64":
		intValue, ok := arg.Value.(int64)
		if ok {
			return intValue, nil
		}
	}

	return 0, typeConversionError(arg.Value, argType)
}

func getUintValue(arg *TaskArg) (uint64, error) {
	argType := typesMap[arg.Type].String()

	switch argType {
	case "uint":
		uintValue, ok := arg.Value.(uint)
		if ok {
			return uint64(uintValue), nil
		}
	case "uint8":
		uintValue, ok := arg.Value.(uint8)
		if ok {
			return uint64(uintValue), nil
		}
	case "uint16":
		uintValue, ok := arg.Value.(uint16)
		if ok {
			return uint64(uintValue), nil
		}
	case "uint32":
		uintValue, ok := arg.Value.(uint32)
		if ok {
			return uint64(uintValue), nil
		}
	case "uint64":
		uintValue, ok := arg.Value.(uint64)
		if ok {
			return uintValue, nil
		}
	}

	return 0, typeConversionError(arg.Value, argType)
}

func getFloatValue(arg *TaskArg) (float64, error) {
	argType := typesMap[arg.Type].String()

	switch argType {
	case "float32":
		floatValue, ok := arg.Value.(float32)
		if ok {
			return float64(floatValue), nil
		}
	case "float64":
		floatValue, ok := arg.Value.(float64)
		if ok {
			return floatValue, nil
		}
	}

	return 0, typeConversionError(arg.Value, argType)
}
