package logger_test

import (
	"testing"

	"github.com/RichardKnop/machinery/v1/logger"
)

func TestDefaultLogger(t *testing.T) {
	logger.Get().Print("should not panic")
}
