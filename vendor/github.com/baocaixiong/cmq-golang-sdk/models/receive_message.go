package models

type ReceiveMessageReq struct {
	*Request
	QueueName          string `name:"queueName"`
	PollingWaitSeconds *int   `name:"pollingWaitSeconds"`
}

type ReceiveMessageResp struct {
	*Response
	MsgBody          string `json:"msgBody"`
	ReceiptHandle    string `json:"receiptHandle"`
	EnqueueTime      int    `json:"enqueueTime"`
	FirstDequeueTime int    `json:"firstDequeueTime"`
	NextVisibleTime  int    `json:"nextVisibleTime"`
	DequeueCount     int    `json:"dequeueCount"`
}

func NewReceiveMessageReq(n string) *ReceiveMessageReq {
	return &ReceiveMessageReq{
		QueueName: n,
		Request:   InitReq("ReceiveMessage", "queue"),
	}
}

func NewReceiveMessageResp() *ReceiveMessageResp {
	return new(ReceiveMessageResp)
}
