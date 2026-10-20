package result_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/backends/eager"
	"github.com/RichardKnop/machinery/v1/backends/result"
	"github.com/RichardKnop/machinery/v1/tasks"
)

func TestResultTimeoutInterruptsPollingSleep(t *testing.T) {
	for _, ready := range []bool{false, true} {
		for _, kind := range []string{"task", "chain", "chord"} {
			name := kind + "/pending"
			if ready {
				name = kind + "/ready"
			}
			t.Run(name, func(t *testing.T) {
				backend := eager.New()
				first := &tasks.Signature{UUID: "first", Name: "first"}
				last := &tasks.Signature{UUID: "last", Name: "last"}
				for _, signature := range []*tasks.Signature{first, last} {
					var err error
					if ready {
						err = backend.SetStateSuccess(signature, []*tasks.TaskResult{{Type: "int", Value: 42}})
					} else {
						err = backend.SetStatePending(signature)
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				var get func(time.Duration, time.Duration) ([]reflect.Value, error)
				switch kind {
				case "task":
					get = result.NewAsyncResult(first, backend).GetWithTimeout
				case "chain":
					get = result.NewChainAsyncResult([]*tasks.Signature{first, last}, backend).GetWithTimeout
				case "chord":
					get = result.NewChordAsyncResult([]*tasks.Signature{first}, last, backend).GetWithTimeout
				}
				start := time.Now()
				values, err := get(25*time.Millisecond, 2*time.Second)
				elapsed := time.Since(start)
				if ready {
					if err != nil || len(values) != 1 || values[0].Int() != 42 {
						t.Fatalf("ready result = %v, %v; want 42, nil", values, err)
					}
				} else if !errors.Is(err, result.ErrTimeoutReached) || values != nil {
					t.Fatalf("pending result = %v, %v; want nil, ErrTimeoutReached", values, err)
				}
				if elapsed >= time.Second {
					t.Fatalf("GetWithTimeout took %v with a 25ms timeout and a 2s polling interval", elapsed)
				}
			})
		}
	}
}
