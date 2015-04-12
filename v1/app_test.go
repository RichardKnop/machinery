package v1

import (
	"testing"
)

type testTask struct{}

func (t testTask) Run(
	args []interface{}, kwargs map[string]interface{},
) (interface{}, error) {
	return nil, nil
}

func TestRegisterTasks(t *testing.T) {
	config := Config{
		BrokerURL:    "amqp://guest:guest@localhost:5672/",
		DefaultQueue: "task_queue",
	}

	app := InitApp(&config)
	app.RegisterTasks(map[string]Task{
		"test_task": testTask{},
	})

	if app.GetRegisteredTask("test_task") == nil {
		t.Error("test_task is not registered but it should be")
	}
}

func TestRegisterTask(t *testing.T) {
	config := Config{
		BrokerURL:    "amqp://guest:guest@localhost:5672/",
		DefaultQueue: "task_queue",
	}

	app := InitApp(&config)
	app.RegisterTask("test_task", testTask{})

	if app.GetRegisteredTask("test_task") == nil {
		t.Error("test_task is not registered but it should be")
	}
}
