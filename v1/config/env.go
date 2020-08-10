package config

import (
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

	if loadedCnf.AMQP == nil {
		cnf.AMQP = nil
	}

	return cnf, nil
}
