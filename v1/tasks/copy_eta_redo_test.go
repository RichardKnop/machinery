package tasks_test

import (
	"github.com/RichardKnop/machinery/v1/tasks"
	"testing"
	"time"
)

func TestCopySignaturePreservesDelayedWorkflow(t *testing.T) {
	eta := time.Date(2030, 4, 5, 6, 7, 8, 123, time.FixedZone("offset", 3600))
	source := &tasks.Signature{Name: "delayed", ETA: &eta, OnSuccess: []*tasks.Signature{{Name: "next", ETA: &eta}}}
	copied := tasks.CopySignature(source)
	for _, s := range []*tasks.Signature{copied, copied.OnSuccess[0]} {
		if s.ETA == nil || !s.ETA.Equal(eta) {
			t.Fatalf("copied delayed task ETA=%v, want %v", s.ETA, eta)
		}
		if s.ETA == source.ETA {
			t.Fatal("copy aliases original ETA")
		}
	}
	*copied.ETA = copied.ETA.Add(time.Hour)
	if !source.ETA.Equal(eta) {
		t.Fatal("copy changed source")
	}
	if tasks.CopySignature(&tasks.Signature{Name: "immediate"}).ETA != nil {
		t.Fatal("nil ETA changed")
	}
}
