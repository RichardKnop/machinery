package models

import (
	"fmt"
)

type CmqErr struct {
	Code      int
	Message   string
	RequestId string
}

func (e *CmqErr) Error() string {
	return fmt.Sprintf("[CmqErr] Code=%d,"+
		" Message=%s, RequestId=%s", e.Code, e.Message, e.RequestId)
}

func NewCmqError(code int, message, requestId string) error {
	return &CmqErr{
		Code:      code,
		Message:   message,
		RequestId: requestId,
	}
}

func (e *CmqErr) GetCode() int {
	return e.Code
}

func (e *CmqErr) GetMessage() string {
	return e.Message
}

func (e *CmqErr) GetRequestId() string {
	return e.RequestId
}
