package utils

import (
	"errors"
	"reflect"
)

var (
	ErrNoMatchType     = errors.New("no match type")
	ErrNoPointer       = errors.New("must be interface")
	ErrInvalidArgument = errors.New("invalid arguments")
)

func deepCopy(dst, src reflect.Value) {
	switch src.Kind() {
	case reflect.Interface:
		value := src.Elem()
		if !value.IsValid() {
			return
		}
		newValue := reflect.New(value.Type()).Elem()
		deepCopy(newValue, value)
		dst.Set(newValue)
	case reflect.Ptr:
		value := src.Elem()
		if !value.IsValid() {
			return
		}
		dst.Set(reflect.New(value.Type()))
		deepCopy(dst.Elem(), value)
	case reflect.Map:
		dst.Set(reflect.MakeMap(src.Type()))
		keys := src.MapKeys()
		for _, key := range keys {
			value := src.MapIndex(key)
			newValue := reflect.New(value.Type()).Elem()
			deepCopy(newValue, value)
			dst.SetMapIndex(key, newValue)
		}
	case reflect.Slice:
		dst.Set(reflect.MakeSlice(src.Type(), src.Len(), src.Cap()))
		for i := 0; i < src.Len(); i++ {
			deepCopy(dst.Index(i), src.Index(i))
		}
	case reflect.Struct:
		typeSrc := src.Type()
		for i := 0; i < src.NumField(); i++ {
			value := src.Field(i)
			tag := typeSrc.Field(i).Tag
			if value.CanSet() && tag.Get("deepcopy") != "-" {
				deepCopy(dst.Field(i), value)
			}
		}
	default:
		dst.Set(src)
	}
}

func DeepCopy(dst, src interface{}) error {
	typeDst := reflect.TypeOf(dst)
	typeSrc := reflect.TypeOf(src)
	if typeDst != typeSrc {
		return ErrNoMatchType
	}
	if typeSrc.Kind() != reflect.Ptr {
		return ErrNoPointer
	}

	valueDst := reflect.ValueOf(dst).Elem()
	valueSrc := reflect.ValueOf(src).Elem()
	if !valueDst.IsValid() || !valueSrc.IsValid() {
		return ErrInvalidArgument
	}

	deepCopy(valueDst, valueSrc)
	return nil
}

func DeepClone(v interface{}) interface{} {
	dst := reflect.New(reflect.TypeOf(v)).Elem()
	deepCopy(dst, reflect.ValueOf(v))
	return dst.Interface()
}
