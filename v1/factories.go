package machinery

import (
	"fmt"
	"strings"

	"github.com/RichardKnop/machinery/v1/brokers"
	"github.com/RichardKnop/machinery/v1/config"
)

// BrokerFactory creates a new object with Broker interface
// Currently only AMQP broker is supported
func BrokerFactory(cnf *config.Config) (brokers.Broker, error) {
	if strings.HasPrefix(cnf.BrokerURL, "amqp://") {
		return brokers.NewAMQPBroker(cnf), nil
	}

	return nil, fmt.Errorf("Factory failed with broker URL: %v", cnf.BrokerURL)
}
