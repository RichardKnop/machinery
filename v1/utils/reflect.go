package utils

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

// ReflectValue converts interface{} to reflect.Value based on string type
func ReflectValue(theType string, value interface{}) (reflect.Value, error) {
	var reflectedValue reflect.Value

	theType = typesMap[theType].String()
	theValue := reflect.New(typesMap[theType])

	// Integers
	if strings.HasPrefix(theType, "int") {
		intValue, err := getIntValue(theType, value)
		if err != nil {
			return reflectedValue, err
		}

		theValue.Elem().SetInt(intValue)
		return theValue.Elem(), err
	}

	// Unbound integers
	if strings.HasPrefix(theType, "uint") {
		uintValue, err := getUintValue(theType, value)
		if err != nil {
			return reflectedValue, err
		}

		theValue.Elem().SetUint(uintValue)
		return theValue.Elem(), err
	}

	// Floating point numbers
	if strings.HasPrefix(theType, "float") {
		floatValue, err := getFloatValue(theType, value)
		if err != nil {
			return reflectedValue, err
		}

		theValue.Elem().SetFloat(floatValue)
		return theValue.Elem(), err
	}

	// Strings
	if theType == "string" {
		stringValue, ok := value.(string)
		if !ok {
			return reflectedValue, typeConversionError(value, theType)
		}

		theValue.Elem().SetString(stringValue)
		return theValue.Elem(), nil
	}

	return reflectedValue, fmt.Errorf("%v is not one of supported types", value)
}

func getIntValue(theType string, value interface{}) (int64, error) {
	argType := typesMap[theType].String()

	switch argType {
	case "int":
		intValue, ok := value.(int)
		if ok {
			return int64(intValue), nil
		}
	case "int8":
		intValue, ok := value.(int8)
		if ok {
			return int64(intValue), nil
		}
	case "int16":
		intValue, ok := value.(int16)
		if ok {
			return int64(intValue), nil
		}
	case "int32":
		intValue, ok := value.(int32)
		if ok {
			return int64(intValue), nil
		}
	case "int64":
		intValue, ok := value.(int64)
		if ok {
			return intValue, nil
		}
	}

	return 0, typeConversionError(value, argType)
}

func getUintValue(theType string, value interface{}) (uint64, error) {
	argType := typesMap[theType].String()

	switch argType {
	case "uint":
		uintValue, ok := value.(uint)
		if ok {
			return uint64(uintValue), nil
		}
	case "uint8":
		uintValue, ok := value.(uint8)
		if ok {
			return uint64(uintValue), nil
		}
	case "uint16":
		uintValue, ok := value.(uint16)
		if ok {
			return uint64(uintValue), nil
		}
	case "uint32":
		uintValue, ok := value.(uint32)
		if ok {
			return uint64(uintValue), nil
		}
	case "uint64":
		uintValue, ok := value.(uint64)
		if ok {
			return uintValue, nil
		}
	}

	return 0, typeConversionError(value, argType)
}

func getFloatValue(theType string, value interface{}) (float64, error) {
	argType := typesMap[theType].String()

	switch argType {
	case "float32":
		floatValue, ok := value.(float32)
		if ok {
			return float64(floatValue), nil
		}
	case "float64":
		floatValue, ok := value.(float64)
		if ok {
			return floatValue, nil
		}
	}

	return 0, typeConversionError(value, argType)
}
