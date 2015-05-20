package machinery

import (
	"fmt"
	"strings"

	"github.com/RichardKnop/machinery/v1/backends"
	"github.com/RichardKnop/machinery/v1/brokers"
	"github.com/RichardKnop/machinery/v1/config"
)

// BrokerFactory creates a new object with brokers.Broker interface
// Currently only AMQP broker is supported
func BrokerFactory(cnf *config.Config, quit chan int) (brokers.Broker, error) {
	if strings.HasPrefix(cnf.Broker, "amqp://") {
		return brokers.NewAMQPBroker(cnf, quit), nil
	}

	return nil, fmt.Errorf("Factory failed with broker URL: %v", cnf.Broker)
}

// BackendFactory creates a new object with backends.Backend interface
// Currently supported backends are AMQP and Memcache
func BackendFactory(cnf *config.Config) (backends.Backend, error) {
	if cnf.ResultBackend == "amqp" {
		return backends.NewAMQPBackend(cnf), nil
	}

	if strings.HasPrefix(cnf.ResultBackend, "memcache://") {
		serversString := strings.Split(cnf.ResultBackend, "memcache://")[1]
		servers := strings.Split(serversString, ",")
		return backends.NewMemcacheBackend(cnf, servers), nil
	}

	return nil, fmt.Errorf("Factory failed with result backend: %v", cnf.ResultBackend)
}
