package models

type ClearSubscriptionFilterTagsReq struct {
	*Request
	TopicName        string `name:"topicName"`
	SubscriptionName string `name:"subscriptionName"`
}

type ClearSubscriptionFilterTagsResp struct {
	*Response
}

func NewClearSubscriptionFilterTagsReq(topicName string, subscriptionName string) *ClearSubscriptionFilterTagsReq {
	return &ClearSubscriptionFilterTagsReq{
		TopicName:        topicName,
		SubscriptionName: subscriptionName,
		Request:          InitReq("ClearSubscriptionFilterTags", "topic"),
	}
}

func NewClearSubscriptionFilterTagsResp() *ClearSubscriptionFilterTagsResp {
	return new(ClearSubscriptionFilterTagsResp)
}
