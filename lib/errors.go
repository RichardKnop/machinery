package lib

import (
	"fmt"
	"log"
)

// FailOnError logs the error and exits the program
// Only use this to handle critical errors
func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

// LogError ogs the error but doesn't exit the program
// Use this to log errors that should not exit the program
func LogError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
