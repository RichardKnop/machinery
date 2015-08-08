package utils

import (
	"fmt"
	"reflect"
	"strings"
)

var (
	typesMap = map[string]reflect.Type{
		"bool":    reflect.TypeOf(true),
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

	// Booleans
	if theType == "bool" {
		boolValue, ok := value.(bool)
		if !ok {
			return reflectedValue, typeConversionError(value, theType)
		}

		theValue.Elem().SetBool(boolValue)
		return theValue.Elem(), nil
	}

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
	// Any numbers from unmarshalled JSON will be float64 by default
	// So we first need to do a type conversion to float64
	number, ok := value.(float64)
	if !ok {
		return 0, typeConversionError(value, typesMap[theType].String())
	}

	// Now we can cast the float64 to int64
	return int64(number), nil
}

func getUintValue(theType string, value interface{}) (uint64, error) {
	// Any numbers from unmarshalled JSON will be float64 by default
	// So we first need to do a type conversion to float64
	number, ok := value.(float64)
	if !ok {
		return 0, typeConversionError(value, typesMap[theType].String())
	}

	// Now we can cast the float64 to uint64
	return uint64(number), nil
}

func getFloatValue(theType string, value interface{}) (float64, error) {
	// Any numbers from unmarshalled JSON will be float64 by default
	// So we first need to do a type conversion to float64
	number, ok := value.(float64)
	if !ok {
		return 0, typeConversionError(value, typesMap[theType].String())
	}

	// Now we can return float64
	return number, nil
}
