package machinery

import (
	"errors"
	"reflect"
	"testing"

	"github.com/RichardKnop/machinery/v1/backends"
	"github.com/RichardKnop/machinery/v1/brokers"
	"github.com/RichardKnop/machinery/v1/config"
)

func TestBrokerFactory(t *testing.T) {
	var cnf config.Config

	// 1) AMQP broker test

	cnf = config.Config{
		Broker:       "amqp://guest:guest@localhost:5672/",
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

	// 1) Redis broker test

	// with password
	cnf = config.Config{
		Broker:       "redis://password@localhost:6379",
		DefaultQueue: "machinery_tasks",
	}

	actual, err = BrokerFactory(&cnf)
	if err != nil {
		t.Errorf(err.Error())
	}

	expected = brokers.NewRedisBroker(&cnf, "localhost:6379", "password")
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("conn = %v, want %v", actual, expected)
	}

	// without password
	cnf = config.Config{
		Broker:       "redis://localhost:6379",
		DefaultQueue: "machinery_tasks",
	}

	actual, err = BrokerFactory(&cnf)
	if err != nil {
		t.Errorf(err.Error())
	}

	expected = brokers.NewRedisBroker(&cnf, "localhost:6379", "")
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("conn = %v, want %v", actual, expected)
	}
}

func TestBrokerFactoryError(t *testing.T) {
	cnf := config.Config{
		Broker: "BOGUS",
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

func TestBackendFactory(t *testing.T) {
	var cnf config.Config

	// 1) AMQP backend test

	cnf = config.Config{ResultBackend: "amqp://guest:guest@localhost:5672/"}
	actual, err := BackendFactory(&cnf)

	if err != nil {
		t.Errorf(err.Error())
	}

	expected := backends.NewAMQPBackend(&cnf)

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("conn = %v, want %v", actual, expected)
	}

	// 2) Memcache backend test

	cnf = config.Config{
		ResultBackend: "memcache://10.0.0.1:11211,10.0.0.2:11211",
	}
	actual, err = BackendFactory(&cnf)

	if err != nil {
		t.Errorf(err.Error())
	}

	servers := []string{"10.0.0.1:11211", "10.0.0.2:11211"}
	expected = backends.NewMemcacheBackend(&cnf, servers)

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("conn = %v, want %v", actual, expected)
	}

	// 2) Redis backend test

	// with password
	cnf = config.Config{
		ResultBackend: "redis://password@localhost:6379",
	}
	actual, err = BackendFactory(&cnf)

	if err != nil {
		t.Errorf(err.Error())
	}

	expected = backends.NewRedisBackend(&cnf, "localhost:6379", "password")

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("conn = %v, want %v", actual, expected)
	}

	// without password
	cnf = config.Config{
		ResultBackend: "redis://localhost:6379",
	}
	actual, err = BackendFactory(&cnf)

	if err != nil {
		t.Errorf(err.Error())
	}

	expected = backends.NewRedisBackend(&cnf, "localhost:6379", "")

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("conn = %v, want %v", actual, expected)
	}

	// 4) MongoDB backend test

	cnf = config.Config{
		ResultBackend: "mongodb://localhost:27017/tasks",
	}
	actual, err = BackendFactory(&cnf)

	if err != nil {
		t.Errorf(err.Error())
	}
	if actual == nil {
		t.Errorf("MongoDB backend is nil")
	}
}

func TestBackendFactoryError(t *testing.T) {
	cnf := config.Config{
		ResultBackend: "BOGUS",
	}

	conn, err := BackendFactory(&cnf)

	if conn != nil {
		t.Errorf("conn = %v, should be nil", conn)
	}

	expectedErr := errors.New("Factory failed with result backend: BOGUS")
	if err.Error() != expectedErr.Error() {
		t.Errorf("err = %v, want %v", err, expectedErr)
	}
}
