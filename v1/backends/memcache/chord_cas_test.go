package memcache

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	gomemcache "github.com/bradfitz/gomemcache/memcache"
	"os"
	"sync"
	"testing"
	"time"
)

func TestTriggerChordCAS(t *testing.T) {
	address := os.Getenv("MEMCACHE_URL")
	if address == "" {
		t.Skip("MEMCACHE_URL is not defined")
	}
	backend := New(&config.Config{}, []string{address})
	const workers = 16
	for round := 0; round < 20; round++ {
		id := fmt.Sprintf("cas-%s-%d", t.Name(), round)
		if err := backend.InitGroup(id, []string{"task"}); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		outcomes := make(chan bool, workers)
		failures := make(chan error, workers)
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				triggered, err := backend.TriggerChord(id)
				outcomes <- triggered
				failures <- err
			}()
		}
		close(start)
		wg.Wait()
		close(outcomes)
		close(failures)
		winners := 0
		for triggered := range outcomes {
			if triggered {
				winners++
			}
		}
		for err := range failures {
			if err != nil {
				t.Error(err)
			}
		}
		if winners != 1 {
			t.Errorf("round %d: %d callers triggered the same chord; want 1", round, winners)
		}
		triggered, err := backend.TriggerChord(id)
		if triggered || err != nil {
			t.Errorf("repeat triggered=%v error=%v; want false,nil", triggered, err)
		}
		if err := backend.PurgeGroupMeta(id); err != nil {
			t.Fatal(err)
		}
	}
}
func TestTriggerChordExistingLockAndMissingGroup(t *testing.T) {
	address := os.Getenv("MEMCACHE_URL")
	if address == "" {
		t.Skip("MEMCACHE_URL is not defined")
	}
	b := New(&config.Config{}, []string{address}).(*Backend)
	missing := "cas-missing-group"
	b.PurgeGroupMeta(missing)
	if triggered, err := b.TriggerChord(missing); triggered || !errors.Is(err, gomemcache.ErrCacheMiss) {
		t.Fatalf("missing group: triggered=%v error=%v", triggered, err)
	}
	id := "cas-existing-lock"
	defer b.PurgeGroupMeta(id)
	meta := &tasks.GroupMeta{GroupUUID: id, Lock: true}
	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.getClient().Set(&gomemcache.Item{Key: id, Value: data}); err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		triggered bool
		err       error
	}
	done := make(chan outcome, 1)
	go func() { triggered, err := b.TriggerChord(id); done <- outcome{triggered, err} }()
	select {
	case result := <-done:
		t.Fatalf("triggered while existing lock held: %+v", result)
	case <-time.After(30 * time.Millisecond):
	}
	meta.Lock = false
	data, err = json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.getClient().Set(&gomemcache.Item{Key: id, Value: data}); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if !result.triggered || result.err != nil {
			t.Fatalf("after unlock: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("trigger did not resume after unlock")
	}
}
