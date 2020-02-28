package errs

import (
	"errors"
	"fmt"
)

// ErrCouldNotUnmarshaTaskSignature ...
type ErrCouldNotUnmarshaTaskSignature struct {
	msg    []byte
	reason string
}

// Error implements the error interface
func (e ErrCouldNotUnmarshaTaskSignature) Error() string {
	return fmt.Sprintf("Could not unmarshal '%s' into a task signature: %v", e.msg, e.reason)
}

// NewErrCouldNotUnmarshaTaskSignature returns new ErrCouldNotUnmarshaTaskSignature instance
func NewErrCouldNotUnmarshaTaskSignature(msg []byte, err error) ErrCouldNotUnmarshaTaskSignature {
	return ErrCouldNotUnmarshaTaskSignature{msg: msg, reason: err.Error()}
}

// ErrConsumerStopped indicates that the operation is now illegal because of the consumer being stopped.
var ErrConsumerStopped = errors.New("the server has been stopped")

// ErrStopTaskDeletion indicates that the task should not be deleted from source after task failure
var ErrStopTaskDeletion = errors.New("task should not be deleted")
