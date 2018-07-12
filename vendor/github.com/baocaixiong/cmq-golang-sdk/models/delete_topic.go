package models

type DeleteTopicReq struct {
	*Request
	TopicName string `name:"topicName"`
}

type DeleteTopicResp struct {
	*Response
}

func NewDeleteTopicReq(topicName string) *DeleteTopicReq {
	return &DeleteTopicReq{
		TopicName: topicName,
		Request:   InitReq("DeleteTopic", "topic"),
	}
}

func NewDeleteTopicResp() *DeleteTopicResp {
	return new(DeleteTopicResp)
}
