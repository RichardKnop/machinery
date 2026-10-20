package amqp_test

import (
	"fmt"
	backend "github.com/RichardKnop/machinery/v2/backends/amqp"
	"github.com/RichardKnop/machinery/v2/config"
	rabbit "github.com/rabbitmq/amqp091-go"
	"os"
	"sync"
	"testing"
	"time"
)

func TestChordClaimNative(t *testing.T) {
	url := os.Getenv("AMQP_URL")
	if url == "" {
		t.Skip("AMQP_URL is not defined")
	}
	conn, err := rabbit.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	b := backend.New(&config.Config{ResultBackend: url, ResultsExpireIn: 60})
	for round := 0; round < 10; round++ {
		group := fmt.Sprintf("chord-native-%d-%d", time.Now().UnixNano(), round)
		if err := b.InitGroup(group, nil); err != nil {
			t.Fatal(err)
		}
		ch, err := conn.Channel()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = ch.QueueDeclare(group, false, false, false, false, nil); err != nil {
			t.Fatal(err)
		}
		ch.Close()
		var wg sync.WaitGroup
		wins := make(chan bool, 16)
		errs := make(chan error, 16)
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); ok, e := b.TriggerChord(group); wins <- ok; errs <- e }()
		}
		wg.Wait()
		close(wins)
		close(errs)
		count := 0
		for ok := range wins {
			if ok {
				count++
			}
		}
		for e := range errs {
			if e != nil {
				t.Error(e)
			}
		}
		if count != 1 {
			t.Errorf("round %d: %d winners, want 1", round, count)
		}
		if e := b.PurgeGroupMeta(group); e != nil {
			t.Error(e)
		}
		if ok, e := b.TriggerChord(group); e != nil || ok {
			t.Errorf("late claim after purge: %v %v", ok, e)
		}
		if e := b.InitGroup(group, nil); e != nil {
			t.Fatal(e)
		}
		if ok, e := b.TriggerChord(group); e != nil || !ok {
			t.Errorf("new generation claim: %v %v", ok, e)
		}
		ch, e := conn.Channel()
		if e != nil {
			t.Fatal(e)
		}
		if _, e = ch.QueueDelete(group+"_chord_triggered", false, false, false); e != nil {
			t.Error(e)
		}
		ch.Close()
	}
}

func TestChordClaimQueueError(t *testing.T) {
	url := os.Getenv("AMQP_URL")
	if url == "" {
		t.Skip("AMQP_URL is not defined")
	}
	conn, e := rabbit.Dial(url)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	ch, e := conn.Channel()
	if e != nil {
		t.Fatal(e)
	}
	defer ch.Close()
	group := fmt.Sprintf("chord-conflict-%d", time.Now().UnixNano())
	marker := group + "_chord_triggered"
	if _, e = ch.QueueDeclare(marker, false, false, false, false, nil); e != nil {
		t.Fatal(e)
	}
	defer ch.QueueDelete(marker, false, false, false)
	b := backend.New(&config.Config{ResultBackend: url, ResultsExpireIn: 60})
	if ok, e := b.TriggerChord(group); e == nil || ok {
		t.Errorf("conflicting marker claim: %v %v", ok, e)
	}
	if e := b.InitGroup(group, nil); e == nil {
		t.Error("conflicting marker initialization succeeded")
	}
	if _, e = ch.QueueInspect(marker); e != nil {
		t.Errorf("existing queue was removed: %v", e)
	}
}
