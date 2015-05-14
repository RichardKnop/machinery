package machinery

import (
	"errors"
	"reflect"
	"testing"

	"github.com/RichardKnop/machinery/v1/brokers"
	"github.com/RichardKnop/machinery/v1/config"
)

func TestBrokerFactory(t *testing.T) {
	cnf := config.Config{
		BrokerURL:    "amqp://guest:guest@localhost:5672/",
		Exchange:     "machinery_exchange",
		ExchangeType: "direct",
		DefaultQueue: "machinery_tasks",
		BindingKey:   "machinery_task",
	}

	actual, err := BrokerFactory(&cnf)

	if err != nil {
		t.Errorf(err.Error())
	}

	expected := brokers.NewAMQPBroker(&cnf)
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("conn = %v, want %v", actual, expected)
	}
}

func TestBrokerFactoryError(t *testing.T) {
	cnf := config.Config{
		BrokerURL: "BOGUS",
	}

	conn, err := BrokerFactory(&cnf)

	if conn != nil {
		t.Errorf("conn = %v, should be nil", conn)
	}

	expectedErr := errors.New("Factory failed with broker URL: BOGUS")
	if err.Error() != expectedErr.Error() {
		t.Errorf("err = %v, want %v", err, expectedErr)
	}
}
