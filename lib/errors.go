package lib

import (
	"fmt"
	"log"
)

type CommandArgumentsError struct {}
func (e CommandArgumentsError) Error() string {
	return "Invalid command arguments"
}

type ArgumentNotString struct {}
func (e ArgumentNotString) Error() string {
	return "Argument must be string"
}

func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}