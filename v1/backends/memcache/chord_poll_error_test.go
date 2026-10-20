package memcache

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/RichardKnop/machinery/v1/config"
	client "github.com/bradfitz/gomemcache/memcache"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestTriggerChordPollingReadError(t *testing.T) {
	for _, scenario := range []string{"locked then read error", "initial read error", "already triggered"} {
		t.Run(scenario, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan struct{})
			connections := make(chan net.Conn, 1)
			var calls int32
			go func() {
				defer close(done)
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				connections <- conn
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				reader := bufio.NewReader(conn)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					fields := strings.Fields(line)
					if len(fields) != 2 || (fields[0] != "get" && fields[0] != "gets") || fields[1] != "group" {
						t.Errorf("unexpected command %q", line)
						return
					}
					n := atomic.AddInt32(&calls, 1)
					if scenario == "initial read error" || n > 1 {
						fmt.Fprint(conn, "END\r\n")
						continue
					}
					data := `{"GroupUUID":"group","Lock":true,"ChordTriggered":false}`
					if scenario == "already triggered" {
						data = `{"GroupUUID":"group","Lock":false,"ChordTriggered":true}`
					}
					fmt.Fprintf(conn, "VALUE group 0 %d\r\n%s\r\nEND\r\n", len(data), data)
				}
			}()
			c := client.New(listener.Addr().String())
			c.Timeout = time.Second
			b := New(&config.Config{}, []string{listener.Addr().String()}).(*Backend)
			b.client = c
			// Close the client-owned pooled connection without accessing shared services.
			defer func() {
				listener.Close()
				select {
				case conn := <-connections:
					conn.Close()
				default:
				}
				select {
				case <-done:
				case <-time.After(6 * time.Second):
					t.Error("fixture did not close")
				}
			}()
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("TriggerChord panicked after a read error: %v", p)
				}
			}()
			triggered, err := b.TriggerChord("group")
			if scenario == "already triggered" {
				if triggered || err != nil {
					t.Fatalf("triggered=%v error=%v", triggered, err)
				}
				return
			}
			if triggered || !errors.Is(err, client.ErrCacheMiss) {
				t.Fatalf("triggered=%v error=%v; want cache miss", triggered, err)
			}
			want := int32(1)
			if scenario == "locked then read error" {
				want = 2
			}
			if got := atomic.LoadInt32(&calls); got != want {
				t.Fatalf("read calls=%d; want %d", got, want)
			}
		})
	}
}
