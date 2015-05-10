package machinery

import (
	"fmt"
	"strings"

	"github.com/RichardKnop/machinery/v1/config"
)

// ConnectionFactory creates a new Connectable object
// Currently only AMQP is supported
func ConnectionFactory(cnf *config.Config) (Connectable, error) {
	if strings.HasPrefix(cnf.BrokerURL, "amqp://") {
		return InitAMQPConnection(cnf), nil
	}

	return nil, fmt.Errorf("Factory failed with broker URL: %v", cnf.BrokerURL)
}
