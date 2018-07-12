package models

type UnsubscribeReq struct {
	*Request
	TopicName        string `name:"topicName"`
	SubscriptionName string `name:"subscriptionName"`
}

type UnsubscribeResp struct {
	*Response
}

func NewUnsubscribeReq(topicName, subscriptionName string) *UnsubscribeReq {
	return &UnsubscribeReq{
		TopicName:        topicName,
		SubscriptionName: subscriptionName,
		Request:          InitReq("Unsubscribe", "topic"),
	}
}

func NewUnsubscribeResp() *UnsubscribeResp {
	return new(UnsubscribeResp)
}
