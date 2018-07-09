package models

type ListSubscriptionByTopicReq struct {
	*Request
	TopicName  string `name:"topicName"`
	SearchWord string `name:"searchWord"`
	Offset     *int   `name:"offset"`
	Limit      *int   `name:"limit"`
}

type ListSubscriptionByTopicResp struct {
	*Response
	TotalCount       int          `json:"totalCount"`
	SubscriptionList []*TopicList `json:"subscriptionList"`
}

type TopicList struct {
	SubscriptionId   string `json:"subscriptionId"`
	SubscriptionName string `json:"subscriptionName"`
	Protocol         string `json:"protocol"`
	Endpoint         string `json:"endpoint"`
}

func NewListSubscriptionByTopicReq(topicName string) *ListSubscriptionByTopicReq {
	return &ListSubscriptionByTopicReq{
		TopicName: topicName,
		Request:   InitReq("ListSubscriptionByTopic", "topic"),
	}
}

func NewListSubscriptionByTopicResp() *ListSubscriptionByTopicResp {
	return new(ListSubscriptionByTopicResp)
}
