package models

type SendMessageReq struct {
	*Request
	QueueName    string `name:"queueName"`
	MsgBody      string `name:"msgBody"`
	DelaySeconds *int   `name:"delaySeconds"`
}

type SendMessageResp struct {
	*Response
}

func NewSendMessageReq(queueName, body string) *SendMessageReq {
	return &SendMessageReq{
		QueueName: queueName,
		MsgBody:   body,
		Request:   InitReq("SendMessage", "queue"),
	}
}

func NewSendMessageResp() *SendMessageResp {
	return new(SendMessageResp)
}
