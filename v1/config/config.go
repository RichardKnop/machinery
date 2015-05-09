package config

import (
	"os"

	"github.com/RichardKnop/machinery/v1/errors"
	"gopkg.in/yaml.v2"
)

// Config holds all configuration for our program
type Config struct {
	BrokerURL    string `yaml:"broker_url"`
	Exchange     string `yaml:"exchange"`
	ExchangeType string `yaml:"exchange_type"`
	DefaultQueue string `yaml:"default_queue"`
	BindingKey   string `yaml:"binding_key"`
}

// ReadFromFile reads data from a file
func ReadFromFile(cnfPath string) ([]byte, error) {
	file, err := os.Open(cnfPath)

	// Config file not found
	if err != nil {
		return nil, &errors.ConfigFileNotFound{}
	}

	// Config file found, let's try to read it
	data := make([]byte, 1000)
	count, err := file.Read(data)
	if err != nil {
		return nil, &errors.ConfigFileReadError{}
	}

	return data[:count], nil
}

// ParseYAMLConfig parses YAML data into Config object
func ParseYAMLConfig(data *[]byte, cnf *Config) {
	err := yaml.Unmarshal(*data, &cnf)
	if err != nil {
		errors.Fail(err, "Failed to unmarshal the config")
	}
}
