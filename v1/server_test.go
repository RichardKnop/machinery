package machinery_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/stretchr/testify/assert"
	"fmt"
)

func TestRegisterTasks(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	err := server.RegisterTasks(map[string]interface{}{
		"test_task": func() error { return nil },
	})
	assert.NoError(t, err)

	_, err = server.GetRegisteredTask("test_task")
	assert.NoError(t, err, "test_task is not registered but it should be")
}

func TestRegisterTask(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	err := server.RegisterTask("test_task", func() error { return nil })
	assert.NoError(t, err)

	_, err = server.GetRegisteredTask("test_task")
	assert.NoError(t, err, "test_task is not registered but it should be")
}

func TestGetRegisteredTask(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)
	_, err := server.GetRegisteredTask("test_task")
	assert.Error(t, err, "test_task is registered but it should not be")
}

func TestGetRegisteredTaskNames(t *testing.T) {
	t.Parallel()

	server := getTestServer(t)

	taskName := "test_task"
	err := server.RegisterTask(taskName, func() error { return nil })
	assert.NoError(t, err)

	taskNames := server.GetRegisteredTaskNames()
	assert.Equal(t, 1, len(taskNames))
	assert.Equal(t, taskName, taskNames[0])
}

func getTestServer(t *testing.T) *machinery.Server {
	server, err := machinery.NewServer(&config.Config{
		Broker:        "amqp://guest:guest@localhost:5672/",
		DefaultQueue:  "machinery_tasks",
		ResultBackend: "redis://127.0.0.1:6379",
		AMQP: &config.AMQPConfig{
			Exchange:      "machinery_exchange",
			ExchangeType:  "direct",
			BindingKey:    "machinery_task",
			PrefetchCount: 1,
		},
	})
	if err != nil {
		t.Error(err)
	}
	return server
}

func TestServer_NewCMQServer(t *testing.T) {
	server, err := machinery.NewServer(&config.Config{
		Broker:        "cmq://key:id@bj?net_env=wan",
		DefaultQueue:  "zaiye-bid",
		ResultBackend: "eager",
		CMQ: &config.CMQConfig{
			WaitTimeSeconds: 30,
		},
	})
	if err != nil {
		t.Error(err)
	}
	//server.SendTask(&tasks.Signature{
	//	Name: "test",
	//	Args: []tasks.Arg{
	//		tasks.Arg{
	//			Name:  "f",
	//			Type:  "uint64",
	//			Value: 123,
	//		},
	//	},
	//})

	//e := server.GetBroker().(cmq.TopicSupport).TopicPublish("order", &tasks.Signature{
	//	Name: "test",
	//	Args: []tasks.Arg{
	//		tasks.Arg{
	//			Name:  "f",
	//			Type:  "uint64",
	//			Value: 123,
	//		},
	//	},
	//	RoutingKey: "order.create",
	//})
	//fmt.Println(e)

	server.RegisterTask("test", func(f uint64) error {
		fmt.Println(f, "----")
		return nil
	})

	server.NewWorker("asf", 1).Launch()
}
