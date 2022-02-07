package log

import (
	"github.com/RichardKnop/logging"
)

var (
	logger = logging.New(nil, nil, new(logging.ColouredFormatter))

	// DEBUG ...
	DEBUG = logger[logging.DEBUG]
	// INFO ...
	INFO = logger[logging.INFO]
	// WARNING ...
	WARNING = logger[logging.WARNING]
	// ERROR ...
	ERROR = logger[logging.ERROR]
	// FATAL ...
	FATAL = logger[logging.FATAL]
)

// Set sets a custom logger for all log levels
func Set(l logging.LoggerInterface) {
	DEBUG = l
	INFO = l
	WARNING = l
	ERROR = l
	FATAL = l
}

// SetDebug sets a custom logger for DEBUG level logs
func SetDebug(l logging.LoggerInterface) {
	DEBUG = l
}

// SetInfo sets a custom logger for INFO level logs
func SetInfo(l logging.LoggerInterface) {
	INFO = l
}

// SetWarning sets a custom logger for WARNING level logs
func SetWarning(l logging.LoggerInterface) {
	WARNING = l
}

// SetError sets a custom logger for ERROR level logs
func SetError(l logging.LoggerInterface) {
	ERROR = l
}

// SetFatal sets a custom logger for FATAL level logs
func SetFatal(l logging.LoggerInterface) {
	FATAL = l
}
