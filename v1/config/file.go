package config

import (
	"fmt"
	"os"
	"time"

	"github.com/RichardKnop/machinery/v1/log"
	"gopkg.in/yaml.v2"
)

// NewFromYaml creates a config object from YAML file
func NewFromYaml(cnfPath string, mustLoadOnce, keepReloading bool) *Config {
	if configLoaded {
		return cnf
	}

	// If the config must be loaded once successfully
	if mustLoadOnce && !configLoaded {
		newCnf, err := fromFile(cnfPath)
		if err != nil {
			log.FATAL.Fatal(err)
		}

		// Refresh the config
		Refresh(newCnf)

		// Set configLoaded to true
		configLoaded = true
		log.INFO.Print("Successfully loaded config from file for the first time")
	}

	if keepReloading {
		// Open a goroutine to watch remote changes forever
		go func(theConfigPath string) {
			for {
				// Delay after each request
				<-time.After(reloadDelay)

				// Attempt to reload the config
				newCnf, err := fromFile(theConfigPath)
				if err != nil {
					log.WARNING.Print("Failed to reload config from file: ", err)
					continue
				}

				// Refresh the config
				Refresh(newCnf)

				// Set configLoaded to true
				configLoaded = true
			}
		}(cnfPath)
	}

	return cnf
}

// ReadFromFile reads data from a file
func ReadFromFile(cnfPath string) ([]byte, error) {
	file, err := os.Open(cnfPath)

	// Config file not found
	if err != nil {
		return nil, fmt.Errorf("Open file error: %s", err)
	}

	// Config file found, let's try to read it
	data := make([]byte, 1000)
	count, err := file.Read(data)
	if err != nil {
		return nil, fmt.Errorf("Read from file error: %s", err)
	}

	return data[:count], nil
}

func fromFile(cnfPath string) (*Config, error) {
	var newCnf Config
	newCnf = *cnf

	data, err := ReadFromFile(cnfPath)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(data, &newCnf); err != nil {
		return nil, fmt.Errorf("Unmarshal YAML error: %s", err)
	}

	return &newCnf, nil
}
