package logger

import (
	"log"
	"os"
)

var logger Interface

// Set sets a custom logger which satisfies the interface
func Set(l Interface) {
	logger = l
}

// Get returns the logger, should be used across the codebase to enable
// developers to plug in their own loggers instead of the stdlib one
func Get() Interface {
	if logger == nil {
		stdLogger := log.New(os.Stdout, "machinery: ", log.Lshortfile)
		logger = Interface(stdLogger)
	}
	return logger
}
