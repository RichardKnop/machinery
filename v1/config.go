package v1

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

// Config holds all configuration for our program
type Config struct {
	BrokerURL    string
	DefaultQueue string
}

// ReadFromFile reads data from a file
func ReadFromFile(configPath string) []byte {
	file, err := os.Open(configPath)

	// Config file not found
	if err != nil {
		FailOnError(err,
			fmt.Sprintf(
				"Config file not found at %v",
				configPath))
	}

	// Config file found, let's try to read it
	data := make([]byte, 1000)
	count, err := file.Read(data)
	if err != nil {
		FailOnError(err,
			fmt.Sprintf(
				"Could not read the config file at %s",
				configPath))
	}

	return data[:count]
}

// ParseYAMLConfig parses YAML data into Config object
func ParseYAMLConfig(data *[]byte, config *Config) {
	err := yaml.Unmarshal(*data, &config)
	if err != nil {
		FailOnError(err, "Failed to unmarshal the config")
	}
}
