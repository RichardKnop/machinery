package models

type BatchDeleteMessageReq struct {
	*Request
	QueueName     string   `name:"queueName"`
	ReceiptHandle []string `name:"receiptHandle"`
}

type BatchDeleteMessageResp struct {
	*Response
	ErrorList []DelError `json:"errorList"`
}

type DelError struct {
	Code          int    `json:"code"`
	Message       string `json:"message"`
	ReceiptHandle string `json:"receiptHandle"`
}

func NewBatchDeleteMessageReq(queueName string) *BatchDeleteMessageReq {
	return &BatchDeleteMessageReq{
		QueueName: queueName,
		Request:   InitReq("BatchDeleteMessageReq", "queue"),
	}
}

func NewBatchDeleteMessageResp() *BatchDeleteMessageResp {
	return new(BatchDeleteMessageResp)
}
