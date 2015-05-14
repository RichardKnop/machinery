package machinery

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
)

func TestRegisterTasks(t *testing.T) {
	server, err := NewServer(&config.Config{
		Broker:       "amqp://guest:guest@localhost:5672/",
		Exchange:     "machinery_exchange",
		ExchangeType: "direct",
		DefaultQueue: "machinery_tasks",
		BindingKey:   "machinery_task",
	})
	if err != nil {
		t.Error(err)
	}

	server.RegisterTasks(map[string]interface{}{
		"test_task": func() {},
	})

	if server.GetRegisteredTask("test_task") == nil {
		t.Error("test_task is not registered but it should be")
	}
}

func TestRegisterTask(t *testing.T) {
	server, err := NewServer(&config.Config{
		Broker:       "amqp://guest:guest@localhost:5672/",
		Exchange:     "machinery_exchange",
		ExchangeType: "direct",
		DefaultQueue: "machinery_tasks",
		BindingKey:   "machinery_task",
	})
	if err != nil {
		t.Error(err)
	}

	server.RegisterTask("test_task", func() {})

	if server.GetRegisteredTask("test_task") == nil {
		t.Error("test_task is not registered but it should be")
	}
}
