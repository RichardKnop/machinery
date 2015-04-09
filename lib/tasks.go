package lib

type Task interface {
	Process(kwargs map[string]interface{})
}