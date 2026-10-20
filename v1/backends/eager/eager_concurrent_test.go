package eager_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/RichardKnop/machinery/v1/backends/eager"
	"github.com/RichardKnop/machinery/v1/tasks"
)

func TestConcurrentTaskAndGroupLifecycles(t *testing.T) {
	backend := eager.New()
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for iteration := 0; iteration < 25; iteration++ {
				id := fmt.Sprintf("task-%d-%d", worker, iteration)
				group := fmt.Sprintf("group-%d-%d", worker, iteration)
				signature := &tasks.Signature{UUID: id, Name: "concurrent-task"}
				if err := backend.SetStatePending(signature); err != nil {
					t.Error(err)
					return
				}
				pending, err := backend.GetState(id)
				if err != nil || pending == nil || pending.State != tasks.StatePending {
					t.Errorf("pending state = %v, %v", pending, err)
					return
				}
				if err := backend.InitGroup(group, []string{id}); err != nil {
					t.Error(err)
					return
				}
				if err := backend.SetStateSuccess(signature, nil); err != nil {
					t.Error(err)
					return
				}
				completed, err := backend.GroupCompleted(group, 1)
				if err != nil || !completed {
					t.Errorf("completed = %v, %v", completed, err)
					return
				}
				states, err := backend.GroupTaskStates(group, 1)
				if err != nil || len(states) != 1 || states[0].TaskUUID != id || !states[0].IsSuccess() {
					t.Errorf("group states = %v, %v", states, err)
					return
				}
				if err := backend.PurgeGroupMeta(group); err != nil {
					t.Error(err)
					return
				}
				if err := backend.PurgeState(id); err != nil {
					t.Error(err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
}
