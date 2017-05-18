package tasks

import (
	"context"
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

	ctxType = reflect.TypeOf((*context.Context)(nil)).Elem()

	typeConversionError = func(argValue interface{}, argTypeStr string) error {
		return fmt.Errorf("%v is not %v", argValue, argTypeStr)
	}
)

// ErrUnsupportedType ...
type ErrUnsupportedType struct {
	valueType string
}

// NewErrUnsupportedType returns new ErrUnsupportedType
func NewErrUnsupportedType(valueType string) ErrUnsupportedType {
	return ErrUnsupportedType{valueType}
}

// Error method so we implement the error interface
func (e ErrUnsupportedType) Error() string {
	return fmt.Sprintf("%v is not one of supported types", e.valueType)
}

// ReflectValue converts interface{} to reflect.Value based on string type
func ReflectValue(valueType string, value interface{}) (reflect.Value, error) {
	theType, ok := typesMap[valueType]
	if !ok {
		return reflect.Value{}, NewErrUnsupportedType(valueType)
	}
	theValue := reflect.New(theType)

	// Booleans
	if theType.String() == "bool" {
		boolValue, ok := value.(bool)
		if !ok {
			return reflect.Value{}, typeConversionError(value, theType.String())
		}

		theValue.Elem().SetBool(boolValue)
		return theValue.Elem(), nil
	}

	// Integers
	if strings.HasPrefix(theType.String(), "int") {
		intValue, err := getIntValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}

		theValue.Elem().SetInt(intValue)
		return theValue.Elem(), err
	}

	// Unbound integers
	if strings.HasPrefix(theType.String(), "uint") {
		uintValue, err := getUintValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}

		theValue.Elem().SetUint(uintValue)
		return theValue.Elem(), err
	}

	// Floating point numbers
	if strings.HasPrefix(theType.String(), "float") {
		floatValue, err := getFloatValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}

		theValue.Elem().SetFloat(floatValue)
		return theValue.Elem(), err
	}

	// Strings
	if theType.String() == "string" {
		stringValue, ok := value.(string)
		if !ok {
			return reflect.Value{}, typeConversionError(value, theType.String())
		}

		theValue.Elem().SetString(stringValue)
		return theValue.Elem(), nil
	}

	return reflect.Value{}, NewErrUnsupportedType(valueType)
}

func getIntValue(theType string, value interface{}) (int64, error) {
	if strings.HasPrefix(fmt.Sprintf("%T", value), "float") {
		// Any numbers from unmarshalled JSON will be float64 by default
		// So we first need to do a type conversion to float64
		n, ok := value.(float64)
		if !ok {
			return 0, typeConversionError(value, typesMap[theType].String())
		}

		// Now we can cast the float64 to int64
		return int64(n), nil
	}

	n, ok := value.(int64)
	if !ok {
		return 0, typeConversionError(value, typesMap[theType].String())
	}

	return n, nil
}

func getUintValue(theType string, value interface{}) (uint64, error) {
	if strings.HasPrefix(fmt.Sprintf("%T", value), "float") {
		// Any numbers from unmarshalled JSON will be float64 by default
		// So we first need to do a type conversion to float64
		n, ok := value.(float64)
		if !ok {
			return 0, typeConversionError(value, typesMap[theType].String())
		}

		// Now we can cast the float64 to uint64
		return uint64(n), nil
	}

	n, ok := value.(uint64)
	if !ok {
		return 0, typeConversionError(value, typesMap[theType].String())
	}

	return n, nil
}

func getFloatValue(theType string, value interface{}) (float64, error) {
	n, ok := value.(float64)
	if !ok {
		return 0, typeConversionError(value, typesMap[theType].String())
	}

	return n, nil
}

// IsContextType checks to see if the type is a context.Context
func IsContextType(t reflect.Type) bool {
	return t == ctxType
}
