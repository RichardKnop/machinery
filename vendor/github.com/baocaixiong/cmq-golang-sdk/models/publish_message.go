package models

type PublishMessageReq struct {
	*Request
	TopicName  string    `name:"topicName"`
	MsgBody    string    `name:"msgBody"`
	MsgTag     *[]string `name:"msgTag"`
	RoutingKey *string   `name:"routingKey"`
}

type PublishMessageResp struct {
	*Response
}

func NewPublishMessageReq(t, body string) *PublishMessageReq {
	return &PublishMessageReq{
		TopicName: t,
		MsgBody:   body,
		Request:   InitReq("PublishMessage", "topic"),
	}
}

func NewPublishMessageResp() *PublishMessageResp {
	return new(PublishMessageResp)
}
