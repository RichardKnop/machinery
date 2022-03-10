package tasks

import (
	"time"

	"github.com/RichardKnop/machinery/v1/retry"
)

type SignatureInterface interface {
	GetSig() *Signature
	GetCommon() *CommonSignature
	RetryTimeout() int
}

type CommonSignature struct {
	CreateTime time.Time `bson:"createTime,omitempty"`
	UpdateTime time.Time `bson:"updateTime,omitempty"`
	State      string    `bson:"state,omitempty"`
}

type TransNotification struct {
	EvoTransID string `bson:"evoTransID,omitempty"`
	// MsgTye      int    `bson:"msgTye,omitempty"`      // 0-异步通知 1-Push通知 2-Email通知 3-SMS通知
	FromChanMsg string `bson:"fromChanMsg,omitempty"` // 渠道异步消息报文（内部字段）
	ToMerMsg    string `bson:"toMerMsg,omitempty"`    // 对下异步通知内容（内部字段）
	SendTimes   int    `bson:"sendTimes,omitempty"`   // 发送次数
	Destination string `bson:"destination,omitempty"` // 消息投递目的地
}

// trans notification signature
type NotificationSignature struct {
	*Signature
	CommonSignature    `bson:",inline"`
	*TransNotification `bson:",inline"`
	intervals          retry.TransExponentialBackoff
}

func NewNotificationSignature(name string, args []Arg, tn *TransNotification) *NotificationSignature {
	sig, _ := NewSignature(name, args)
	sig.MsgType = NOTIFICATION

	create := time.Now()

	ns := NotificationSignature{
		Signature: sig,
		CommonSignature: CommonSignature{
			CreateTime: create,
			UpdateTime: create,
			State:      StateCreated,
		},
		TransNotification: tn,
		intervals:         retry.NotificationInternals,
	}
	ns.RetryCount = ns.intervals.RetryCount()
	return &ns
}

func (ns *NotificationSignature) GetSig() *Signature {
	return ns.Signature
}

func (ns *NotificationSignature) GetCommon() *CommonSignature {
	return &ns.CommonSignature
}

func (ns *NotificationSignature) RetryTimeout() int {
	return 0
}
