package models

type DeleteQueueReq struct {
	*Request
	QueueName string `name:"queueName"`
}

type DeleteQueueResp struct {
	*Response
}

func NewDeleteQueueReq(queueName string) *DeleteQueueReq {
	return &DeleteQueueReq{
		QueueName: queueName,
		Request:   InitReq("DeleteQueue", "queue"),
	}
}

func NewDeleteQueueResp() *DeleteQueueResp {
	return new(DeleteQueueResp)
}
