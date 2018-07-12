package models

type RewindQueueReq struct {
	*Request
	QueueName        string `name:"queueName"`
	StartConsumeTime int    `name:"startConsumeTime"` // Unix时间戳
}

type RewindQueueResp struct {
	*Response
}

func NewRewindQueueReq(queueName string, startConsumeTime int) *RewindQueueReq {
	return &RewindQueueReq{
		Request:          InitReq("RewindQueue", "queue"),
		QueueName:        queueName,
		StartConsumeTime: startConsumeTime,
	}
}

func NewRewindQueueResp() *RewindQueueResp {
	return new(RewindQueueResp)
}
