package errs

import (
	"errors"
	"fmt"
)

// ErrCouldNotUnmarshalTaskSignature ...
type ErrCouldNotUnmarshalTaskSignature struct {
	msg    []byte
	reason string
}

// Error implements the error interface
func (e ErrCouldNotUnmarshalTaskSignature) Error() string {
	return fmt.Sprintf("Could not unmarshal '%s' into a task signature: %v", e.msg, e.reason)
}

// NewErrCouldNotUnmarshalTaskSignature returns new ErrCouldNotUnmarshalTaskSignature instance
func NewErrCouldNotUnmarshalTaskSignature(msg []byte, err error) ErrCouldNotUnmarshalTaskSignature {
	return ErrCouldNotUnmarshalTaskSignature{msg: msg, reason: err.Error()}
}

// ErrConsumerStopped indicates that the operation is now illegal because of the consumer being stopped.
var ErrConsumerStopped = errors.New("the server has been stopped")

// ErrStopTaskDeletion indicates that the task should not be deleted from source after task failure
var ErrStopTaskDeletion = errors.New("task should not be deleted")
