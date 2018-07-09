package models

type CreateTopicReq struct {
	*Request
	TopicName  string `name:"topicName"`
	MaxMsgSize int    `name:"maxMsgSize"`
	FilterType *int   `name:"maxMsgSize"`
}

type CreateTopicResp struct {
	*Response
	TopicId string `json:"topicId"`
}

func NewCreateTopicReq(topicName string) *CreateTopicReq {
	return &CreateTopicReq{
		TopicName: topicName,
		Request:   InitReq("CreateTopic", "topic"),
	}
}

func NewCreateTopicResp() *CreateTopicResp {
	return new(CreateTopicResp)
}
