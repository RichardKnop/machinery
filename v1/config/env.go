package config

import (
	"os"

	"github.com/kelseyhightower/envconfig"

	"github.com/RichardKnop/machinery/v1/log"
)

// NewFromEnvironment creates a config object from environment variables
func NewFromEnvironment() (*Config, error) {
	cnf, err := fromEnvironment()
	if err != nil {
		return nil, err
	}

	log.INFO.Print("Successfully loaded config from the environment")

	return cnf, nil
}

func fromEnvironment() (*Config, error) {
	loadedCnf, cnf := new(Config), new(Config)
	*cnf = *defaultCnf

	if err := envconfig.Process("", cnf); err != nil {
		return nil, err
	}
	if err := envconfig.Process("", loadedCnf); err != nil {
		return nil, err
	}

	// Keep the historical misspelling as a fallback for existing deployments.
	if _, present := os.LookupEnv("MULTIPLE_BROKER_SEPARATOR"); !present {
		if separator, present := os.LookupEnv("MULTIPLE_BROKEN_SEPARATOR"); present {
			cnf.MultipleBrokerSeparator = separator
		}
	}

	if loadedCnf.AMQP == nil {
		cnf.AMQP = nil
	}

	return cnf, nil
}
