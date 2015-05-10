package machinery

import (
	"errors"
	"reflect"
	"testing"

	"github.com/RichardKnop/machinery/v1/config"
)

func TestConnectionFactory(t *testing.T) {
	cnf := config.Config{
		BrokerURL:    "amqp://guest:guest@localhost:5672/",
		Exchange:     "machinery_exchange",
		ExchangeType: "direct",
		DefaultQueue: "machinery_tasks",
		BindingKey:   "machinery_task",
	}

	actual, err := ConnectionFactory(&cnf)

	if err != nil {
		t.Errorf(err.Error())
	}

	expected := AMQPConnection{config: &cnf}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("conn = %v, want %v", actual, expected)
	}
}

func TestConnectionFactoryError(t *testing.T) {
	cnf := config.Config{
		BrokerURL: "BOGUS",
	}

	conn, err := ConnectionFactory(&cnf)

	if conn != nil {
		t.Errorf("conn = %v, should be nil", conn)
	}

	expectedErr := errors.New("Factory failed with broker URL: BOGUS")
	if err.Error() != expectedErr.Error() {
		t.Errorf("err = %v, want %v", err, expectedErr)
	}
}
