package models

type SetQueueAttributesReq struct {
	*Request
	QueueName           string `name:"queueName"`
	MaxMsgHeapNum       *int   `name:"maxMsgHeapNum"`
	PollingWaitSeconds  *int   `name:"pollingWaitSeconds"`
	VisibilityTimeout   *int   `name:"visibilityTimeout"`
	MaxMsgSize          *int   `name:"maxMsgSize"`
	MsgRetentionSeconds *int   `name:"msgRetentionSeconds"`
	RewindSeconds       *int   `name:"rewindSeconds"`
}

type SetQueueAttributesResp struct {
	*Response
	MaxMsgHeapNum       int `json:"maxMsgHeapNum"`
	PollingWaitSeconds  int `json:"pollingWaitSeconds"`
	VisibilityTimeout   int `json:"visibilityTimeout"`
	MaxMsgSize          int `json:"maxMsgSize"`
	MsgRetentionSeconds int `json:"msgRetentionSeconds"`
	RewindSeconds       int `json:"rewindSeconds"`
}

func NewSetQueueAttributesReq(queueName string) *SetQueueAttributesReq {
	return &SetQueueAttributesReq{
		QueueName: queueName,
		Request:   InitReq("SetQueueAttributes", "queue"),
	}
}

func NewSetQueueAttributesResp() *SetQueueAttributesResp {
	return new(SetQueueAttributesResp)
}
