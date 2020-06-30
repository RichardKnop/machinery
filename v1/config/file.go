package config

import (
	"fmt"
	"os"
	"time"

	"github.com/RichardKnop/machinery/v1/log"
	"gopkg.in/yaml.v2"
)

// NewFromYaml creates a config object from YAML file
func NewFromYaml(cnfPath string, keepReloading bool) (*Config, error) {
	cnf, err := fromFile(cnfPath)
	if err != nil {
		return nil, err
	}

	log.INFO.Printf("Successfully loaded config from file %s", cnfPath)

	if keepReloading {
		// Open a goroutine to watch remote changes forever
		go func() {
			for {
				// Delay after each request
				time.Sleep(reloadDelay)

				// Attempt to reload the config
				newCnf, newErr := fromFile(cnfPath)
				if newErr != nil {
					log.WARNING.Printf("Failed to reload config from file %s: %v", cnfPath, newErr)
					continue
				}

				*cnf = *newCnf
			}
		}()
	}

	return cnf, nil
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
	loadedCnf, cnf := new(Config), new(Config)
	*cnf = *defaultCnf

	data, err := ReadFromFile(cnfPath)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(data, cnf); err != nil {
		return nil, fmt.Errorf("Unmarshal YAML error: %s", err)
	}
	if err := yaml.Unmarshal(data, loadedCnf); err != nil {
		return nil, fmt.Errorf("Unmarshal YAML error: %s", err)
	}
	if loadedCnf.AMQP == nil {
		cnf.AMQP = nil
	}

	return cnf, nil
}
