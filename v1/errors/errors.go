package errors

import (
	"log"
)

// Fail logs the error and exits the program
// Only use this to handle critical errors
func Fail(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

// Log only logs the error but doesn't exit the program
// Use this to log errors that should not exit the program
func Log(err error, msg string) {
	if err != nil {
		log.Printf("[ERROR] %s: %s", msg, err)
	}
}
