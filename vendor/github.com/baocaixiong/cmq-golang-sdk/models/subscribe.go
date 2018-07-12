package models

type SubscribeReq struct {
	*Request
	TopicName           string    `name:"topicName"`
	SubscriptionName    string    `name:"subscriptionName"`
	Protocol            string    `name:"protocol"`
	Endpoint            string    `name:"endpoint"`
	NotifyStrategy      *string   `name:"notifyStrategy"`
	NotifyContentFormat *string   `name:"notifyContentFormat"`
	FilterTag           *[]string `name:"filterTag"`
	BindingKey          []string  `name:"bindingKey"`
}

type SubscribeResp struct {
	*Response
}

func NewSubscribeRep(q, sname, protocol, endpoint string) *SubscribeReq {
	return &SubscribeReq{
		TopicName:        q,
		SubscriptionName: sname,
		Protocol:         protocol,
		Endpoint:         endpoint,
		Request:          InitReq("Subscribe", "topic"),
	}
}

func NewSubscribeResp() *SubscribeResp {
	return &SubscribeResp{}
}
