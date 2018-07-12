package models

type SetSubscriptionAttributesReq struct {
	*Request
	TopicName           string    `name:"topicName"`
	SubscriptionName    string    `name:"subscriptionName"`
	NotifyStrategy      *string   `name:"notifyStrategy"`
	NotifyContentFormat *string   `name:"notifyContentFormat"`
	FilterTag           *[]string `name:"filterTag"`
	BindingKey          []string  `name:"bindingKey"`
}

type SetSubscriptionAttributesResp struct {
	*Response
}

func NewSetSubscriptionAttributesReq(topicName, subscriptionName string) *SetSubscriptionAttributesReq {
	return &SetSubscriptionAttributesReq{
		TopicName:        topicName,
		SubscriptionName: subscriptionName,
		Request:          InitReq("SetSubscriptionAttributes", "topic"),
	}
}

func NewSetSubscriptionAttributesResp() *SetSubscriptionAttributesResp {
	return new(SetSubscriptionAttributesResp)
}
