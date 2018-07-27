package models

type ListQueueReq struct {
	*Request
	SearchWord string `name:"searchWord"`
	Offset     *int   `name:"offset"`
	Limit      *int   `name:"limit"`
}

type ListQueueResp struct {
	*Response
	TotalCount int      `json:"totalCount"`
	QueueList  []*Queue `json:"queueList"`
}

type Queue struct {
	QueueId   string `json:"queueId"`
	QueueName string `json:"queueName"`
}

func NewListQueueReq() *ListQueueReq {
	return &ListQueueReq{
		Request: InitReq("ListQueue", "queue"),
	}
}

func NewListQueueResp() *ListQueueResp {
	return new(ListQueueResp)
}
