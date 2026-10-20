package result_test

import (
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v2/backends/eager"
	"github.com/RichardKnop/machinery/v2/backends/result"
	"github.com/RichardKnop/machinery/v2/tasks"
)

func TestChordGetWithTimeoutPropagatesTaskErrors(t *testing.T) {
	for _, tc := range []struct {
		name, groupError, callbackError, wantError string
	}{
		{name: "group task failed", groupError: "group failed", wantError: "group failed"},
		{name: "callback failed", callbackError: "callback failed", wantError: "callback failed"},
		{name: "successful callback"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := eager.New()
			groupTask := &tasks.Signature{UUID: "group-task", Name: "group-task"}
			callback := &tasks.Signature{UUID: "callback", Name: "callback"}
			for _, task := range []struct {
				signature *tasks.Signature
				failure   string
			}{{groupTask, tc.groupError}, {callback, tc.callbackError}} {
				var err error
				if task.failure != "" {
					err = backend.SetStateFailure(task.signature, task.failure)
				} else {
					err = backend.SetStateSuccess(task.signature, []*tasks.TaskResult{{Type: "int", Value: 42}})
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			chord := result.NewChordAsyncResult([]*tasks.Signature{groupTask}, callback, backend)
			values, err := chord.GetWithTimeout(time.Second, time.Millisecond)
			if tc.wantError != "" {
				if err == nil || err.Error() != tc.wantError || values != nil {
					t.Fatalf("GetWithTimeout() = %v, %v; want nil, %q", values, err, tc.wantError)
				}
			} else if err != nil || len(values) != 1 || values[0].Int() != 42 {
				t.Fatalf("GetWithTimeout() = %v, %v; want callback result 42", values, err)
			}
		})
	}
}
