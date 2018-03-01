package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

var (
	typesMap = map[string]reflect.Type{
		// base types
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
		// slices
		"[]bool":    reflect.TypeOf(make([]bool, 0)),
		"[]int":     reflect.TypeOf(make([]int, 0)),
		"[]int8":    reflect.TypeOf(make([]int8, 0)),
		"[]int16":   reflect.TypeOf(make([]int16, 0)),
		"[]int32":   reflect.TypeOf(make([]int32, 0)),
		"[]int64":   reflect.TypeOf(make([]int64, 0)),
		"[]uint":    reflect.TypeOf(make([]uint, 0)),
		"[]uint8":   reflect.TypeOf(make([]uint8, 0)),
		"[]uint16":  reflect.TypeOf(make([]uint16, 0)),
		"[]uint32":  reflect.TypeOf(make([]uint32, 0)),
		"[]uint64":  reflect.TypeOf(make([]uint64, 0)),
		"[]float32": reflect.TypeOf(make([]float32, 0)),
		"[]float64": reflect.TypeOf(make([]float64, 0)),
		"[]string":  reflect.TypeOf([]string{""}),
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
	if strings.HasPrefix(valueType, "[]") {
		return reflectValues(valueType, value)
	}

	return reflectValue(valueType, value)
}

// reflectValue converts interface{} to reflect.Value based on string type
// representing a base type (not a slice)
func reflectValue(valueType string, value interface{}) (reflect.Value, error) {
	theType, ok := typesMap[valueType]
	if !ok {
		return reflect.Value{}, NewErrUnsupportedType(valueType)
	}
	theValue := reflect.New(theType)

	// Booleans
	if theType.String() == "bool" {
		boolValue, err := getBoolValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
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

	// Unsigned integers
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
		stringValue, err := getStringValue(theType.String(), value)
		if err != nil {
			return reflect.Value{}, err
		}

		theValue.Elem().SetString(stringValue)
		return theValue.Elem(), nil
	}

	return reflect.Value{}, NewErrUnsupportedType(valueType)
}

// reflectValues converts interface{} to reflect.Value based on string type
// representing a slice of values
func reflectValues(valueType string, value interface{}) (reflect.Value, error) {
	theType, ok := typesMap[valueType]
	if !ok {
		return reflect.Value{}, NewErrUnsupportedType(valueType)
	}
	var theValue reflect.Value

	values, ok := value.([]interface{})
	if !ok {
		return reflect.Value{}, typeConversionError(value, theType.String())
	}

	// Booleans
	if theType.String() == "[]bool" {
		bools, err := getBoolValues(strings.Split(theType.String(), "[]")[1], values)
		if err != nil {
			return reflect.Value{}, err
		}

		theValue = reflect.MakeSlice(theType, len(bools), len(bools))
		for i, v := range bools {
			theValue.Index(i).SetBool(v)
		}

		return theValue, nil
	}

	// Integers
	if strings.HasPrefix(theType.String(), "[]int") {
		ints, err := getIntValues(strings.Split(theType.String(), "[]")[1], values)
		if err != nil {
			return reflect.Value{}, err
		}

		theValue = reflect.MakeSlice(theType, len(ints), len(ints))
		for i, v := range ints {
			theValue.Index(i).SetInt(v)
		}

		return theValue, nil
	}

	// Unsigned integers
	if strings.HasPrefix(theType.String(), "[]uint") {
		uints, err := getUintValues(strings.Split(theType.String(), "[]")[1], values)
		if err != nil {
			return reflect.Value{}, err
		}

		theValue = reflect.MakeSlice(theType, len(uints), len(uints))
		for i, v := range uints {
			theValue.Index(i).SetUint(v)
		}

		return theValue, nil
	}

	// Floating point numbers
	if strings.HasPrefix(theType.String(), "[]float") {
		floats, err := getFloatValues(strings.Split(theType.String(), "[]")[1], values)
		if err != nil {
			return reflect.Value{}, err
		}

		theValue = reflect.MakeSlice(theType, len(floats), len(floats))
		for i, v := range floats {
			theValue.Index(i).SetFloat(v)
		}

		return theValue, nil
	}

	// Strings
	if theType.String() == "[]string" {
		strs, err := getStringValues(strings.Split(theType.String(), "[]")[1], values)
		if err != nil {
			return reflect.Value{}, err
		}

		theValue = reflect.MakeSlice(theType, len(strs), len(strs))
		for i, v := range strs {
			theValue.Index(i).SetString(v)
		}

		return theValue, nil
	}

	return reflect.Value{}, NewErrUnsupportedType(valueType)
}

func getBoolValue(theType string, value interface{}) (bool, error) {
	b, ok := value.(bool)
	if !ok {
		return false, typeConversionError(value, typesMap[theType].String())
	}

	return b, nil
}

func getIntValue(theType string, value interface{}) (int64, error) {
	// We use https://golang.org/pkg/encoding/json/#Decoder.UseNumber when unmarshaling signatures.
	// This is because JSON only supports 64-bit floating point numbers and we could lose precision
	// when converting from float64 to signed integer
	if strings.HasPrefix(fmt.Sprintf("%T", value), "json.Number") {
		n, ok := value.(json.Number)
		if !ok {
			return 0, typeConversionError(value, typesMap[theType].String())
		}

		return n.Int64()
	}

	n, ok := value.(int64)
	if !ok {
		return 0, typeConversionError(value, typesMap[theType].String())
	}

	return n, nil
}

func getUintValue(theType string, value interface{}) (uint64, error) {
	// We use https://golang.org/pkg/encoding/json/#Decoder.UseNumber when unmarshaling signatures.
	// This is because JSON only supports 64-bit floating point numbers and we could lose precision
	// when converting from float64 to unsigned integer
	if strings.HasPrefix(fmt.Sprintf("%T", value), "json.Number") {
		n, ok := value.(json.Number)
		if !ok {
			return 0, typeConversionError(value, typesMap[theType].String())
		}

		intVal, err := n.Int64()
		if err != nil {
			return 0, err
		}

		return uint64(intVal), nil
	}

	n, ok := value.(uint64)
	if !ok {
		return 0, typeConversionError(value, typesMap[theType].String())
	}

	return n, nil
}

func getFloatValue(theType string, value interface{}) (float64, error) {
	// We use https://golang.org/pkg/encoding/json/#Decoder.UseNumber when unmarshaling signatures.
	// This is because JSON only supports 64-bit floating point numbers and we could lose precision
	if strings.HasPrefix(fmt.Sprintf("%T", value), "json.Number") {
		n, ok := value.(json.Number)
		if !ok {
			return 0, typeConversionError(value, typesMap[theType].String())
		}

		return n.Float64()
	}

	f, ok := value.(float64)
	if !ok {
		return 0, typeConversionError(value, typesMap[theType].String())
	}

	return f, nil
}

func getStringValue(theType string, value interface{}) (string, error) {
	s, ok := value.(string)
	if !ok {
		return "", typeConversionError(value, typesMap[theType].String())
	}

	return s, nil
}

func getBoolValues(theType string, values []interface{}) ([]bool, error) {
	bools := make([]bool, len(values))
	for i, v := range values {
		boolValue, err := getBoolValue(theType, v)
		if err != nil {
			return []bool{}, err
		}
		bools[i] = boolValue
	}

	return bools, nil
}

func getIntValues(theType string, values []interface{}) ([]int64, error) {
	ints := make([]int64, len(values))
	for i, v := range values {
		intValue, err := getIntValue(theType, v)
		if err != nil {
			return []int64{}, err
		}
		ints[i] = intValue
	}

	return ints, nil
}

func getUintValues(theType string, values []interface{}) ([]uint64, error) {
	uints := make([]uint64, len(values))
	for i, v := range values {
		uintValue, err := getUintValue(theType, v)
		if err != nil {
			return []uint64{}, err
		}
		uints[i] = uintValue
	}

	return uints, nil
}

func getFloatValues(theType string, values []interface{}) ([]float64, error) {
	floats := make([]float64, len(values))
	for i, v := range values {
		floatValue, err := getFloatValue(theType, v)
		if err != nil {
			return []float64{}, err
		}
		floats[i] = floatValue
	}

	return floats, nil
}

func getStringValues(theType string, values []interface{}) ([]string, error) {
	strs := make([]string, len(values))
	for i, v := range values {
		stringValue, err := getStringValue(theType, v)
		if err != nil {
			return []string{}, err
		}
		strs[i] = stringValue
	}

	return strs, nil
}

// IsContextType checks to see if the type is a context.Context
func IsContextType(t reflect.Type) bool {
	return t == ctxType
}
