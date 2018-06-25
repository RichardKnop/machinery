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

// Set sets a custom logger
func Set(l logging.LoggerInterface) {
	DEBUG = l
	INFO = l
	WARNING = l
	ERROR = l
	FATAL = l
}
