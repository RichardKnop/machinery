package amqp

import (
	"errors"
	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	amqp "github.com/rabbitmq/amqp091-go"
	"testing"
)

type processingAckObserver struct {
	acks, nacks, rejects int
	tag                  uint64
	multiple             bool
}

func (a *processingAckObserver) Ack(tag uint64, multiple bool) error {
	a.acks++
	a.tag = tag
	a.multiple = multiple
	return nil
}
func (a *processingAckObserver) Nack(uint64, bool, bool) error { a.nacks++; return nil }
func (a *processingAckObserver) Reject(uint64, bool) error     { a.rejects++; return nil }

type processingErrorProcessor struct {
	iface.TaskProcessor
	err   error
	calls int
}

func (p *processingErrorProcessor) Process(*tasks.Signature) error { p.calls++; return p.err }
func TestConsumeOneProcessingAcknowledgement(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		ack      bool
		wantAcks int
	}{
		{"processing failure", errors.New("state persistence unavailable"), true, 0},
		{"retain failed task", errs.ErrStopTaskDeletion, true, 0},
		{"success", nil, true, 1},
		{"inspection success", nil, false, 0},
		{"inspection failure", errors.New("inspection failed"), false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := New(&config.Config{}).(*Broker)
			b.SetRegisteredTaskNames([]string{"registered"})
			observer := &processingAckObserver{}
			processor := &processingErrorProcessor{err: tc.err}
			delivery := amqp.Delivery{Body: []byte(`{"Name":"registered"}`), DeliveryTag: 42, Acknowledger: observer}
			err := b.consumeOne(delivery, processor, tc.ack)
			if err != tc.err {
				t.Fatalf("error=%v; want original %v", err, tc.err)
			}
			if processor.calls != 1 {
				t.Fatalf("Process calls=%d; want 1", processor.calls)
			}
			if observer.acks != tc.wantAcks || observer.nacks != 0 || observer.rejects != 0 {
				t.Fatalf("ack=%d nack=%d reject=%d; want ack=%d only", observer.acks, observer.nacks, observer.rejects, tc.wantAcks)
			}
			if tc.wantAcks == 1 && (observer.tag != 42 || observer.multiple) {
				t.Fatalf("ack tag=%d multiple=%v", observer.tag, observer.multiple)
			}
		})
	}
}
