package config

import (
	"time"

	"github.com/RichardKnop/machinery/v1/log"
	"github.com/kelseyhightower/envconfig"
)

// NewFromEnvironment creates a config object from environment variables
func NewFromEnvironment(mustLoadOnce, keepReloading bool) *Config {
	if configLoaded {
		return cnf
	}

	// If the config must be loaded once successfully
	if mustLoadOnce && !configLoaded {
		newCnf, err := fromEnvironment()
		if err != nil {
			log.FATAL.Fatal(err)
		}

		// Refresh the config
		Refresh(newCnf)

		// Set configLoaded to true
		configLoaded = true
		log.INFO.Print("Successfully loaded config from environment for the first time")
	}

	if keepReloading {
		// Open a goroutine to watch remote changes forever
		go func() {
			for {
				// Delay after each request
				<-time.After(reloadDelay)

				// Attempt to reload the config
				newCnf, err := fromEnvironment()
				if err != nil {
					log.WARNING.Print("Failed to reload config from environment: ", err)
					continue
				}

				// Refresh the config
				Refresh(newCnf)

				// Set configLoaded to true
				configLoaded = true
			}
		}()
	}

	return cnf
}

func fromEnvironment() (*Config, error) {
	var newCnf Config
	newCnf = *cnf

	if err := envconfig.Process("", &newCnf); err != nil {
		return nil, err
	}

	return &newCnf, nil
}
