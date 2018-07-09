package models

type BatchReceiveMessageReq struct {
	*Request
	QueueName          string `name:"queueName"`
	NumOfMsg           int    `name:"numOfMsg"`
	PollingWaitSeconds *int   `name:"pollingWaitSeconds"`
}

type BatchReceiveMessageResp struct {
	*Response
	MsgInfoList []*MsgInfo `json:"msgInfoList"`
}

type MsgInfo struct {
	MsgBody          string `json:"msgBody"`
	MsgId            string `json:"msgId"`
	ReceiptHandle    string `json:"receiptHandle"`
	EnqueueTime      int    `json:"enqueueTime"`
	FirstDequeueTime int    `json:"firstDequeueTime"`
	NextVisibleTime  int    `json:"nextVisibleTime"`
	DequeueCount     int    `json:"dequeueCount"`
}

func NewBatchReceiveMessageReq(queueName string, numOfMsg int) *BatchReceiveMessageReq {
	return &BatchReceiveMessageReq{
		QueueName: queueName,
		Request:   InitReq("BatchReceiveMessage", "queue"),
	}
}

func NewBatchReceiveMessageResp() *BatchReceiveMessageResp {
	return new(BatchReceiveMessageResp)
}
