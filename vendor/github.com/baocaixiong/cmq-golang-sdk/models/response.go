package models

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
)

type IResponse interface {
	GetCode() int
	GetMessage() string
	GetRequestId() string
}

type Response struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	RequestId string `json:"requestId"`
	MsgId     string `json:"msgId"`
}

func (r *Response) GetCode() int {
	return r.Code
}
func (r *Response) GetMessage() string {
	return r.Message
}

func (r *Response) GetRequestId() string {
	return r.RequestId
}

func ParseFromHttpResponse(hr *http.Response, response IResponse) (err error) {
	defer hr.Body.Close()
	body, err := ioutil.ReadAll(hr.Body)
	if err != nil {
		return
	}
	err = json.Unmarshal(body, &response)
	if err != nil {
		return err
	}
	return
}
