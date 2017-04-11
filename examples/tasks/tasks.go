package exampletasks

import (
	"errors"
	"encoding/json"
)

// Add ...
func Add(args ...int64) (int64, error) {
	sum := int64(0)
	for _, arg := range args {
		sum += arg
	}
	return sum, nil
}

// Multiply ...
func Multiply(args ...int64) (int64, error) {
	sum := int64(1)
	for _, arg := range args {
		sum *= arg
	}
	return sum, nil
}

// PanicTask ...
func PanicTask() (string, error) {
	panic(errors.New("oops"))
}

//Custom type
type Line struct {
	Point1 Coordinates
	Point2 Coordinates
}

func (l Line) InJson() string {
	res, _ := json.Marshal(l)
	return string(res)
}

type Coordinates struct {
	X float64
	Y float64
}

//Calculate slope of a line
func LineSlope(line Line) (float64, error) {
	slope := (line.Point2.Y - line.Point1.Y) / (line.Point2.X - line.Point1.X)
	return slope, nil
}