package models

type BatchPublishMessageReq struct {
	*Request
	TopicName  string    `name:"topicName"`
	MsgBody    []string  `name:"msgBody"`
	MsgTag     *[]string `name:"msgTag"`
	RoutingKey *string   `name:"routingKey"`
}

type BatchPublishMessageResp struct {
	*Response
	MsgList []*PublishMsgId `json:"msgList"`
}

type PublishMsgId struct {
	MsgId string `json:"msgId"`
}

func NewBatchPublishMessageReq(topicName string) *BatchPublishMessageReq {
	return &BatchPublishMessageReq{
		TopicName: topicName,
		Request:   InitReq("BatchPublishMessage", "topic"),
	}
}

func NewBatchPublishMessageResp() *BatchPublishMessageResp {
	return new(BatchPublishMessageResp)
}
