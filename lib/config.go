package lib

import (
	"os"
	"gopkg.in/yaml.v2"
)

func ParseConfig(configMap *map[string]string, configPath *string) {
	file, err := os.Open(*configPath)

	// Config file not found, just return
	if err != nil {
		return
	}
	
	// Config file found, let's try to read it
	data := make([]byte, 1000)
	count, err := file.Read(data)
	if err != nil {
		FailOnError(err, "Failed to read config file")
	}

	err = yaml.Unmarshal(data[:count], &configMap)
	if err != nil {
		FailOnError(err, "Failed to unmarshal the config")
	}
}