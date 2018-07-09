package models

type ListTopicReq struct {
	*Request
	SearchWord *string `name:"searchWord"`
	Offset     *int    `name:"offset"`
	Limit      *int    `name:"limit"`
}

type ListTopicResp struct {
	*Response
	TotalCount int      `json:"totalCount"`
	TopicList  []*Topic `json:"topicList"`
}

type Topic struct {
	TopicId   string `json:"topicId"`
	TopicName string `json:"topicName"`
}

func NewListTopicReq() *ListTopicReq {
	return &ListTopicReq{
		Request: InitReq("ListTopic", "topic"),
	}
}
func NewListTopicResp() *ListTopicResp {
	return new(ListTopicResp)
}
