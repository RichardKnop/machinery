package models

type DeleteMessageReq struct {
	*Request
	QueueName     string `name:"queueName"`
	ReceiptHandle string `name:"receiptHandle"`
}

type DeleteMessageResp struct {
	*Response
}

func NewDeleteMessageReq(queueName, msgHandler string) *DeleteMessageReq {
	return &DeleteMessageReq{
		QueueName:     queueName,
		ReceiptHandle: msgHandler,
		Request:       InitReq("DeleteMessage", "queue"),
	}
}

func NewDeleteMessageResp() *DeleteMessageResp {
	return &DeleteMessageResp{}
}
