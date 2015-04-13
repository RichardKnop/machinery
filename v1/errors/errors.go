package errors

import (
	"fmt"
	"log"
)

// ConfigFileNotFound is used when the config file is not found
type ConfigFileNotFound struct{}

func (e ConfigFileNotFound) Error() string {
	return "Could not find config file"
}

// ConfigFileReadError is used when the data from config file could not be read
type ConfigFileReadError struct{}

func (e ConfigFileReadError) Error() string {
	return "Could not read config data"
}

// ConnectionFactoryError is used when the connection specified
// by a broker URL is not implemented in the factory method
type ConnectionFactoryError struct{}

func (e ConnectionFactoryError) Error() string {
	return "Connection not implemented"
}

// Fail logs the error and exits the program
// Only use this to handle critical errors
func Fail(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

// Log only logs the error but doesn't exit the program
// Use this to log errors that should not exit the program
func Log(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
