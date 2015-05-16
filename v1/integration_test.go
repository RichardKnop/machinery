package machinery

import (
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/errors"
	"github.com/RichardKnop/machinery/v1/signatures"
)

func TestSendTask(t *testing.T) {
	brokerURL := os.Getenv("AMQP_URL")
	if brokerURL == "" {
		return
	}

	server := setup(brokerURL)
	worker := server.NewWorker("test_worker")

	go func() {
		worker.Launch()
	}()

	task := signatures.TaskSignature{
		Name: "add",
		Args: []signatures.TaskArg{
			signatures.TaskArg{
				Type:  "float64",
				Value: interface{}(1),
			},
			signatures.TaskArg{
				Type:  "float64",
				Value: interface{}(1),
			},
		},
	}

	asyncResult, err := server.SendTask(&task)
	if err != nil {
		t.Error(err)
	}

	result, err := asyncResult.Get()
	if err != nil {
		t.Error(err)
	}

	if result.Interface() != float64(2) {
		t.Errorf(
			"result = %v(%v), want float64(2)",
			result.Type().String(),
			result.Interface(),
		)
	}
}

func TestSendChain(t *testing.T) {
	brokerURL := os.Getenv("AMQP_URL")
	if brokerURL == "" {
		return
	}

	server := setup(brokerURL)
	worker := server.NewWorker("test_worker")

	go func() {
		worker.Launch()
	}()

	task1 := signatures.TaskSignature{
		Name: "add",
		Args: []signatures.TaskArg{
			signatures.TaskArg{
				Type:  "float64",
				Value: interface{}(1),
			},
			signatures.TaskArg{
				Type:  "float64",
				Value: interface{}(1),
			},
		},
	}

	task2 := signatures.TaskSignature{
		Name: "add",
		Args: []signatures.TaskArg{
			signatures.TaskArg{
				Type:  "float64",
				Value: interface{}(5),
			},
			signatures.TaskArg{
				Type:  "float64",
				Value: interface{}(6),
			},
		},
	}

	task3 := signatures.TaskSignature{
		Name: "multiply",
		Args: []signatures.TaskArg{
			signatures.TaskArg{
				Type:  "float64",
				Value: interface{}(4),
			},
		},
	}

	chain := NewChain(&task1, &task2, &task3)
	chainAsyncResult, err := server.SendChain(chain)
	if err != nil {
		t.Error(err)
	}

	result, err := chainAsyncResult.Get()
	if err != nil {
		t.Error(err)
	}

	if result.Interface() != float64(52) {
		t.Errorf(
			"result = %v(%v), want float64(52)",
			result.Type().String(),
			result.Interface(),
		)
	}
}

func setup(brokerURL string) *Server {
	cnf := config.Config{
		Broker:        brokerURL,
		ResultBackend: "amqp",
		Exchange:      "test_exchange",
		ExchangeType:  "direct",
		DefaultQueue:  "test_queue",
		BindingKey:    "test_task",
	}

	server, err := NewServer(&cnf)
	errors.Fail(err, "Could not initialize server")

	tasks := map[string]interface{}{
		"add": func(args ...float64) (float64, error) {
			sum := 0.0
			for _, arg := range args {
				sum += arg
			}
			return sum, nil
		},
		"multiply": func(args ...float64) (float64, error) {
			sum := 1.0
			for _, arg := range args {
				sum *= arg
			}
			return sum, nil
		},
	}
	server.RegisterTasks(tasks)

	return server
}
