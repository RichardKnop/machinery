package machinery

import (
	"log"
	"testing"
)

func TestReflectArgs(t *testing.T) {
	argValues, err := ReflectArgs([]TaskArg{
		TaskArg{
			Type:  "int",
			Value: interface{}(int(1)),
		},
		TaskArg{
			Type:  "int8",
			Value: interface{}(int8(1)),
		},
		TaskArg{
			Type:  "int16",
			Value: interface{}(int16(1)),
		},
		TaskArg{
			Type:  "int32",
			Value: interface{}(int32(1)),
		},
		TaskArg{
			Type:  "int64",
			Value: interface{}(int64(1)),
		},
		TaskArg{
			Type:  "uint",
			Value: interface{}(uint(1)),
		},
		TaskArg{
			Type:  "uint8",
			Value: interface{}(uint8(1)),
		},
		TaskArg{
			Type:  "uint16",
			Value: interface{}(uint16(1)),
		},
		TaskArg{
			Type:  "uint32",
			Value: interface{}(uint32(1)),
		},
		TaskArg{
			Type:  "uint64",
			Value: interface{}(uint64(1)),
		},
		TaskArg{
			Type:  "float32",
			Value: interface{}(float32(0.5)),
		},
		TaskArg{
			Type:  "float64",
			Value: interface{}(float64(0.5)),
		},
		TaskArg{
			Type:  "string",
			Value: interface{}(""),
		},
	})

	if err != nil {
		t.Error(err)
	}

	log.Print(argValues)

	if argValues[0].Type().String() != "int" {
		t.Errorf("arg type is %v, want int", argValues[0].Type().String())
	}

	if argValues[1].Type().String() != "int8" {
		t.Errorf("arg type is %v, want int8", argValues[1].Type().String())
	}

	if argValues[2].Type().String() != "int16" {
		t.Errorf("arg type is %v, want int16", argValues[2].Type().String())
	}

	if argValues[3].Type().String() != "int32" {
		t.Errorf("arg type is %v, want int32", argValues[3].Type().String())
	}

	if argValues[4].Type().String() != "int64" {
		t.Errorf("arg type is %v, want int64", argValues[4].Type().String())
	}

	if argValues[5].Type().String() != "uint" {
		t.Errorf("arg type is %v, want uint", argValues[5].Type().String())
	}

	if argValues[6].Type().String() != "uint8" {
		t.Errorf("arg type is %v, want uint8", argValues[6].Type().String())
	}

	if argValues[7].Type().String() != "uint16" {
		t.Errorf("arg type is %v, want uint16", argValues[7].Type().String())
	}

	if argValues[8].Type().String() != "uint32" {
		t.Errorf("arg type is %v, want uint32", argValues[8].Type().String())
	}

	if argValues[9].Type().String() != "uint64" {
		t.Errorf("arg type is %v, want uint64", argValues[9].Type().String())
	}

	if argValues[10].Type().String() != "float32" {
		t.Errorf("arg type is %v, want float32", argValues[10].Type().String())
	}

	if argValues[11].Type().String() != "float64" {
		t.Errorf("arg type is %v, want float64", argValues[11].Type().String())
	}

	if argValues[12].Type().String() != "string" {
		t.Errorf("arg type is %v, want string", argValues[12].Type().String())
	}
}
