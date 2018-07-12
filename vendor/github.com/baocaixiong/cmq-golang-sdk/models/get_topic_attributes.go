package models

type GetTopicAttributesReq struct {
	*Request
	TopicName string `name:"topicName"`
}

type GetTopicAttributesResp struct {
	*Response
	MsgCount            int `json:"msgCount"`
	MaxMsgSize          int `json:"maxMsgSize"`
	MsgRetentionSeconds int `json:"msgRetentionSeconds"`
	CreateTime          int `json:"createTime"`
	LastModifyTime      int `json:"lastModifyTime"`
	FilterType          int `json:"filterType"`
}

func NewGetTopicAttributesReq(topicName string) *GetTopicAttributesReq {
	return &GetTopicAttributesReq{
		TopicName: topicName,
		Request:   InitReq("NewGetTopicAttributes", "topic"),
	}
}

func NewGetTopicAttributesResp() *GetTopicAttributesResp {
	return new(GetTopicAttributesResp)
}
