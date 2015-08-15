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
func BrokerFactory(cnf *config.Config) (brokers.Broker, error) {
	if strings.HasPrefix(cnf.Broker, "amqp://") {
		return brokers.NewAMQPBroker(cnf), nil
	}

	if strings.HasPrefix(cnf.Broker, "redis://") {
		parts := strings.Split(cnf.Broker, "redis://")
		if len(parts) != 2 {
			return nil, fmt.Errorf(
				"Redis broker connection string should be in format redis://host:port, instead got %s",
				cnf.Broker,
			)
		}
		return brokers.NewRedisBroker(cnf, parts[1]), nil
	}

	return nil, fmt.Errorf("Factory failed with broker URL: %v", cnf.Broker)
}

// BackendFactory creates a new object with backends.Backend interface
// Currently supported backends are AMQP and Memcache
func BackendFactory(cnf *config.Config) (backends.Backend, error) {
	if strings.HasPrefix(cnf.ResultBackend, "amqp://") {
		return backends.NewAMQPBackend(cnf), nil
	}

	if strings.HasPrefix(cnf.ResultBackend, "memcache://") {
		parts := strings.Split(cnf.ResultBackend, "memcache://")
		if len(parts) != 2 {
			return nil, fmt.Errorf(
				"Memcache result backend connection string should be in format memcache://server1:port,server2:port, instead got %s",
				cnf.ResultBackend,
			)
		}
		servers := strings.Split(parts[1], ",")
		return backends.NewMemcacheBackend(cnf, servers), nil
	}

	if strings.HasPrefix(cnf.ResultBackend, "redis://") {
		parts := strings.Split(cnf.ResultBackend, "redis://")
		if len(parts) != 2 {
			return nil, fmt.Errorf(
				"Redis result backend connection string should be in format redis://host:port, instead got %s",
				cnf.ResultBackend,
			)
		}
		return backends.NewRedisBackend(cnf, parts[1]), nil
	}

	return nil, fmt.Errorf("Factory failed with result backend: %v", cnf.ResultBackend)
}
