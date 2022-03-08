package tasks

// 交易异步通知结构体
type NotificationSignature struct {
	*Signature
	EvoTransID  string
	MsgTye      int    // 0-异步通知 1-Push通知 2-Email通知 3-SMS通知
	FromChanMsg string //渠道异步消息报文（内部字段）
	ToMerMsg    string //对下异步通知内容（内部字段）
	SendTimes   int    // 发送次数
	Destination string // 消息投递目的地
}
