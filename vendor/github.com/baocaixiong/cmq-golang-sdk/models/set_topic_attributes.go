package models

type SetTopicAttributesReq struct {
	*Request
	TopicName  string `name:"topicName"`
	MaxMsgSize *int   `name:"maxMsgSize"`
}

type SetTopicAttributesResp struct {
	*Response
}

func NewSetTopicAttributesReq(topicName string) *SetTopicAttributesReq {
	return &SetTopicAttributesReq{
		TopicName: topicName,
		Request:   InitReq("SetTopicAttributes", "topic"),
	}
}

func NewSetTopicAttributesResp() *SetTopicAttributesResp {
	return new(SetTopicAttributesResp)
}
