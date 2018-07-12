package models

type GetSubscriptionAttributesReq struct {
	*Request
	TopicName        string `name:"topicName"`
	SubscriptionName string `name:"subscriptionName"`
}

type GetSubscriptionAttributesResp struct {
	*Response
	TopicOwner          string `json:"topicOwner"`
	MsgCount            int    `json:"msgCount"`
	Protocol            string `json:"protocol"`
	Endpoint            string `json:"endpoint"`
	NotifyStrategy      string `json:"notifyStrategy"`
	NotifyContentFormat string `json:"notifyContentFormat"`
	CreateTime          int    `json:"createTime"`
	LastModifyTime      int    `json:"lastModifyTime"`
	BindingKey          string `json:"bindingKey"`
}

func NewGetSubscriptionAttributesReq(topicName, subscriptionName string) *GetSubscriptionAttributesReq {
	return &GetSubscriptionAttributesReq{
		TopicName:        topicName,
		SubscriptionName: subscriptionName,
		Request:          InitReq("GetSubscriptionAttributes", "topic"),
	}
}

func NewGetSubscriptionAttributesResp() *GetSubscriptionAttributesResp {
	return new(GetSubscriptionAttributesResp)
}
