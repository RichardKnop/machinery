package config

import (
	"time"

	"github.com/RichardKnop/machinery/v1/log"
	"github.com/kelseyhightower/envconfig"
)

// NewFromEnvironment creates a config object from environment variables
func NewFromEnvironment(keepReloading bool) (*Config, error) {
	cnf, err := fromEnvironment()
	if err != nil {
		return nil, err
	}

	log.INFO.Print("Successfully loaded config from the environment")

	if keepReloading {
		// Open a goroutine to watch remote changes forever
		go func() {
			for {
				// Delay after each request
				time.Sleep(reloadDelay)

				// Attempt to reload the config
				newCnf, newErr := fromEnvironment()
				if newErr != nil {
					log.WARNING.Printf("Failed to reload config from the environment: %v", newErr)
					continue
				}

				*cnf = *newCnf
				// log.INFO.Printf("Successfully reloaded config from the environment")
			}
		}()
	}

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
