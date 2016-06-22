package machinery

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
)

func TestRegisterTasks(t *testing.T) {
	server := _getTestServer(t)
	server.RegisterTasks(map[string]interface{}{
		"test_task": func() {},
	})

	_, err := server.GetRegisteredTask("test_task")
	if err != nil {
		t.Error("test_task is not registered but it should be")
	}
}

func TestRegisterTask(t *testing.T) {
	server := _getTestServer(t)
	server.RegisterTask("test_task", func() {})

	_, err := server.GetRegisteredTask("test_task")
	if err != nil {
		t.Error("test_task is not registered but it should be")
	}
}

func TestGetRegisteredTask(t *testing.T) {
	_, err := _getTestServer(t).GetRegisteredTask("test_task")
	if err == nil {
		t.Error("test_task is registered but it should not be")
	}
}

func TestGetRegisteredTaskNames(t *testing.T) {
	server := _getTestServer(t)
	taskName := "test_task"
	server.RegisterTask(taskName, func() {})
	names := server.getRegisteredTaskNames()
	if len(names) != 1 || names[0] != taskName {
		t.Error("server registered one task but got:", len(names), names)
	}
}

func _getTestServer(t *testing.T) *Server {
	server, err := NewServer(&config.Config{
		Broker:        "amqp://guest:guest@localhost:5672/",
		ResultBackend: "redis://127.0.0.1:6379",
		Exchange:      "machinery_exchange",
		ExchangeType:  "direct",
		DefaultQueue:  "machinery_tasks",
		BindingKey:    "machinery_task",
	})
	if err != nil {
		t.Error(err)
	}
	return server
}
