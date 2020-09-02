package tasks

import (
	"fmt"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TaskResult represents an actual return value of a processed task
type TaskResult struct {
	Type  string      `bson:"type"`
	Value interface{} `bson:"value"`
}

// ReflectTaskResults ...
func ReflectTaskResults(taskResults []*TaskResult) ([]reflect.Value, error) {
	resultValues := make([]reflect.Value, len(taskResults))
	for i, taskResult := range taskResults {

		if taskResult.Type == "json" {
			taskResult.Type = "string"
			ret, err := bson.MarshalExtJSON(taskResult.Value.(primitive.D).Map(), false, false)
			// ret, err := json.Marshal(taskResult.Value.(primitive.D).Map())
			if err != nil {
				return nil, err
			}
			taskResult.Value = string(ret)
		}

		resultValue, err := ReflectValue(taskResult.Type, taskResult.Value)
		if err != nil {
			return nil, err
		}
		resultValues[i] = resultValue
	}
	return resultValues, nil
}

// HumanReadableResults ...
func HumanReadableResults(results []reflect.Value) string {
	if len(results) == 1 {
		return fmt.Sprintf("%v", results[0].Interface())
	}

	readableResults := make([]string, len(results))
	for i := 0; i < len(results); i++ {
		readableResults[i] = fmt.Sprintf("%v", results[i].Interface())
	}

	return fmt.Sprintf("[%s]", strings.Join(readableResults, ", "))
}
