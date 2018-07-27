package models

type GetQueueAttributesReq struct {
	*Request
	QueueName string `name:"queueName"`
}

type GetQueueAttributesResp struct {
	*Response
	MaxMsgHeapNum       int `json:"maxMsgHeapNum"`
	PollingWaitSeconds  int `json:"pollingWaitSeconds"`
	VisibilityTimeout   int `json:"visibilityTimeout"`
	MaxMsgSize          int `json:"maxMsgSize"`
	MsgRetentionSeconds int `json:"msgRetentionSeconds"`
	CreateTime          int `json:"createTime"`
	LastModifyTime      int `json:"lastModifyTime"`
	ActiveMsgNum        int `json:"activeMsgNum"`
	InactiveMsgNum      int `json:"inactiveMsgNum"`
	RewindSeconds       int `json:"rewindSeconds"`
	RewindMsgNum        int `json:"rewindmsgNum"`
	MinMsgTime          int `json:"minMsgTime"`
}

func NewGetQueueAttributesReq(queueName string) *GetQueueAttributesReq {
	return &GetQueueAttributesReq{
		QueueName: queueName,
		Request:   InitReq("GetQueueAttributes", "queue"),
	}
}

func NewGetQueueAttributesResp() *GetQueueAttributesResp {
	return new(GetQueueAttributesResp)
}
