package errors

import (
	"fmt"
	"log"
)

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
