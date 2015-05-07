package machinery

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
)

type testTask struct{}

func (t testTask) Run(args []interface{}) (interface{}, error) {
	return nil, nil
}

func TestRegisterTasks(t *testing.T) {
	cnf := config.Config{
		BrokerURL:    "amqp://guest:guest@localhost:5672/",
		DefaultQueue: "task_queue",
	}

	app := InitApp(&cnf)
	app.RegisterTasks(map[string]Task{
		"test_task": testTask{},
	})

	if app.GetRegisteredTask("test_task") == nil {
		t.Error("test_task is not registered but it should be")
	}
}

func TestRegisterTask(t *testing.T) {
	cnf := config.Config{
		BrokerURL:    "amqp://guest:guest@localhost:5672/",
		DefaultQueue: "task_queue",
	}

	app := InitApp(&cnf)
	app.RegisterTask("test_task", testTask{})

	if app.GetRegisteredTask("test_task") == nil {
		t.Error("test_task is not registered but it should be")
	}
}
