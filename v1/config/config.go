package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

// Config holds all configuration for our program
type Config struct {
	Broker          string `yaml:"broker"`
	ResultBackend   string `yaml:"result_backend"`
	ResultsExpireIn int    `yaml:"results_expire_in"`
	Exchange        string `yaml:"exchange"`
	ExchangeType    string `yaml:"exchange_type"`
	DefaultQueue    string `yaml:"default_queue"`
	BindingKey      string `yaml:"binding_key"`
}

// ReadFromFile reads data from a file
func ReadFromFile(cnfPath string) ([]byte, error) {
	file, err := os.Open(cnfPath)

	// Config file not found
	if err != nil {
		return nil, fmt.Errorf("Config Open: %v", err)
	}

	// Config file found, let's try to read it
	data := make([]byte, 1000)
	count, err := file.Read(data)
	if err != nil {
		return nil, fmt.Errorf("Config Read: %v", err)
	}

	return data[:count], nil
}

// ParseYAMLConfig parses YAML data into Config object
func ParseYAMLConfig(data *[]byte, cnf *Config) error {
	err := yaml.Unmarshal(*data, &cnf)
	if err != nil {
		return fmt.Errorf("Config Unmarshal: %v", err)
	}

	return nil
}
