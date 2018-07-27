package models

type QueueCreateReq struct {
	*Request
	QueueName           string            `name:"queueName"`
	MaxMsgHeapNum       *int              `name:"maxMsgHeapNum"`
	PollingWaitSeconds  *int              `name:"pollingWaitSeconds"`
	VisibilityTimeout   *int              `name:"visibilityTimeout"`
	MaxMsgSize          *int              `name:"maxMsgSize"`
	MsgRetentionSeconds *int              `name:"msgRetentionSeconds"`
	RewindSeconds       *int              `name:"rewindSeconds"`
	DeadLetterPolicy    *DeadLetterPolicy `name:"deadLetterPolicy"`
}

type DeadLetterPolicy struct {
	DeadLetterQueue string `json:"deadLetterQueue"`
	Policy          int    `json:"policy"`
	MaxReceiveCount *int   `json:"maxReceiveCount"`
	MaxTimeToLive   *int   `json:"maxTimeToLive"`
}

type QueueCreateResp struct {
	*Response
	QueueId string `json:"queueId"`
}

func NewCreateQueueRequest(n string) *QueueCreateReq {
	return &QueueCreateReq{
		QueueName: n,
		Request:   InitReq("CreateQueue", "queue"),
	}
}

func NewCreateQueueResp() *QueueCreateResp {
	return &QueueCreateResp{}
}
