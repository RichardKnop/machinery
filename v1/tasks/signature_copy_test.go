package tasks_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/tasks"
)

func TestCopySignaturePreservesETA(t *testing.T) {
	for _, tc := range []struct {
		name string
		eta  time.Time
	}{
		{"UTC", time.Date(2030, 4, 5, 6, 7, 8, 9, time.UTC)},
		{"fixed zone", time.Date(2030, 4, 5, 6, 7, 8, 9, time.FixedZone("task", 7200))},
		{"monotonic", time.Now().Add(time.Hour)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			callbackETA := tc.eta.Add(time.Minute)
			original := &tasks.Signature{
				Name: "scheduled", ETA: &tc.eta,
				Headers:   tasks.Headers{"trace": "original"},
				OnSuccess: []*tasks.Signature{{Name: "callback", ETA: &callbackETA}},
			}
			copied := tasks.CopySignature(original)
			if !reflect.DeepEqual(*copied.ETA, *original.ETA) || !reflect.DeepEqual(*copied.OnSuccess[0].ETA, *original.OnSuccess[0].ETA) {
				t.Errorf("copied signature differs: ETA %v, want %v; callback ETA %v, want %v", copied.ETA, original.ETA, copied.OnSuccess[0].ETA, original.OnSuccess[0].ETA)
			}
			if copied.ETA == original.ETA || copied.OnSuccess[0].ETA == original.OnSuccess[0].ETA {
				t.Fatal("copy shares ETA pointers")
			}
			*copied.ETA = copied.ETA.Add(time.Hour)
			*copied.OnSuccess[0].ETA = copied.OnSuccess[0].ETA.Add(time.Hour)
			copied.Headers["trace"] = "copied"
			if !original.ETA.Equal(tc.eta) || !original.OnSuccess[0].ETA.Equal(callbackETA) || original.Headers["trace"] != "original" {
				t.Fatal("changing the copy changed the original")
			}
		})
	}
}

func TestCopySignaturePreservesNilETA(t *testing.T) {
	original := &tasks.Signature{Name: "immediate"}
	copied := tasks.CopySignature(original)
	if copied.ETA != nil || copied.Name != original.Name {
		t.Fatalf("copy of immediate signature = %#v", copied)
	}
}
