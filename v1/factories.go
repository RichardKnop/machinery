package machinery

import (
	"strings"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/errors"
)

// ConnectionFactory creates a new Connectable object
// Currently only AMQP is supported
func ConnectionFactory(cnf *config.Config) (Connectable, error) {
	var conn Connectable
	var err error

	if strings.HasPrefix(cnf.BrokerURL, "amqp://") {
		conn = AMQPConnection{config: cnf}
	} else {
		err = errors.ConnectionFactoryError{}
	}

	return conn, err
}
