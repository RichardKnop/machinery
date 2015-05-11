package machinery

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
)

type testTask struct{}

func TestRegisterTasks(t *testing.T) {
	app, err := InitApp(&config.Config{
		BrokerURL:    "amqp://guest:guest@localhost:5672/",
		Exchange:     "machinery_exchange",
		ExchangeType: "direct",
		DefaultQueue: "machinery_tasks",
		BindingKey:   "machinery_task",
	})
	if err != nil {
		t.Error(err)
	}

	app.RegisterTasks(map[string]interface{}{
		"test_task": func() {},
	})

	if app.GetRegisteredTask("test_task") == nil {
		t.Error("test_task is not registered but it should be")
	}
}

func TestRegisterTask(t *testing.T) {
	app, err := InitApp(&config.Config{
		BrokerURL:    "amqp://guest:guest@localhost:5672/",
		Exchange:     "machinery_exchange",
		ExchangeType: "direct",
		DefaultQueue: "machinery_tasks",
		BindingKey:   "machinery_task",
	})
	if err != nil {
		t.Error(err)
	}

	app.RegisterTask("test_task", func() {})

	if app.GetRegisteredTask("test_task") == nil {
		t.Error("test_task is not registered but it should be")
	}
}
