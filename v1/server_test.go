package machinery_test

import (
	"testing"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/stretchr/testify/assert"
)

func TestRegisterTasks(t *testing.T) {
	server := getTestServer(t)
	server.RegisterTasks(map[string]interface{}{
		"test_task": func() {},
	})

	_, err := server.GetRegisteredTask("test_task")
	assert.NoError(t, err, "test_task is not registered but it should be")
}

func TestRegisterTask(t *testing.T) {
	server := getTestServer(t)
	server.RegisterTask("test_task", func() {})

	_, err := server.GetRegisteredTask("test_task")
	assert.NoError(t, err, "test_task is not registered but it should be")
}

func TestGetRegisteredTask(t *testing.T) {
	server := getTestServer(t)
	_, err := server.GetRegisteredTask("test_task")
	assert.Error(t, err, "test_task is registered but it should not be")
}

func TestGetRegisteredTaskNames(t *testing.T) {
	server := getTestServer(t)
	taskName := "test_task"
	server.RegisterTask(taskName, func() {})
	names := server.GetRegisteredTaskNames()
	assert.Equal(t, 1, len(names))
	assert.Equal(t, taskName, names[0])
}

func getTestServer(t *testing.T) *machinery.Server {
	server, err := machinery.NewServer(&config.Config{
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
