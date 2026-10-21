package redis

import (
	"bufio"
	"fmt"
	"github.com/RichardKnop/machinery/v2/brokers/iface"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/tasks"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type pollProcessor struct{ processed atomic.Int32 }

func (p *pollProcessor) Process(*tasks.Signature) error { p.processed.Add(1); return nil }
func (*pollProcessor) CustomQueue() string              { return "" }
func (*pollProcessor) PreConsumeHandler() bool          { return true }

func TestRedisPollErrorBackoff(t *testing.T) {
	for _, driver := range []string{"redigo", "goredis"} {
		for _, mode := range []string{"error", "empty", "task", "recovery"} {
			t.Run(driver+"/"+mode, func(t *testing.T) {
				listener, e := net.Listen("tcp", "127.0.0.1:0")
				if e != nil {
					t.Fatal(e)
				}
				var wg sync.WaitGroup
				var mu sync.Mutex
				connections := []net.Conn{}
				var calls atomic.Int32
				first := make(chan struct{})
				var once sync.Once
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						conn, e := listener.Accept()
						if e != nil {
							return
						}
						mu.Lock()
						connections = append(connections, conn)
						mu.Unlock()
						wg.Add(1)
						go func() {
							defer wg.Done()
							defer conn.Close()
							r := bufio.NewReader(conn)
							for {
								line, e := r.ReadString('\n')
								if e != nil {
									return
								}
								if !strings.HasPrefix(line, "*") {
									return
								}
								n, e := strconv.Atoi(strings.TrimSpace(line[1:]))
								if e != nil {
									return
								}
								parts := make([]string, n)
								for i := range parts {
									line, e = r.ReadString('\n')
									if e != nil {
										return
									}
									size, e := strconv.Atoi(strings.TrimSpace(line[1:]))
									if e != nil {
										return
									}
									data := make([]byte, size+2)
									if _, e = io.ReadFull(r, data); e != nil {
										return
									}
									parts[i] = string(data[:size])
								}
								reply := "+OK\r\n"
								switch strings.ToUpper(parts[0]) {
								case "HELLO":
									reply = "-ERR unknown command 'hello'\r\n"
								case "PING":
									reply = "+PONG\r\n"
								case "BLPOP":
									count := calls.Add(1)
									once.Do(func() { close(first) })
									if mode == "error" || mode == "recovery" && count == 1 {
										reply = "-WRONGTYPE queue has the wrong kind of value\r\n"
									} else if mode == "task" && count == 1 || mode == "recovery" && count == 2 {
										body := `{"UUID":"poll-task","Name":"poll-task"}`
										reply = fmt.Sprintf("*2\r\n$5\r\nqueue\r\n$%d\r\n%s\r\n", len(body), body)
									} else {
										time.Sleep(20 * time.Millisecond)
										reply = "*-1\r\n"
									}
								case "ZRANGEBYSCORE", "ZREVRANGEBYSCORE":
									reply = "*0\r\n"
								}
								if _, e = io.WriteString(conn, reply); e != nil {
									return
								}
							}
						}()
					}
				}()
				defer func() {
					listener.Close()
					mu.Lock()
					for _, conn := range connections {
						conn.Close()
					}
					mu.Unlock()
					wg.Wait()
				}()
				cfg := &config.Config{DefaultQueue: "queue", Redis: &config.RedisConfig{NormalTasksPollPeriod: 1000, DelayedTasksPollPeriod: 500}}
				var broker iface.Broker
				if driver == "redigo" {
					broker = New(cfg, listener.Addr().String(), "", "", "", 0)
				} else {
					broker = NewGR(cfg, []string{listener.Addr().String()}, 0)
				}
				broker.SetRegisteredTaskNames([]string{"poll-task"})
				processor := new(pollProcessor)
				done := make(chan error, 1)
				go func() { _, e := broker.StartConsuming("poll", 1, processor); done <- e }()
				select {
				case <-first:
				case e := <-done:
					t.Fatalf("consumer ended before BLPOP: %v", e)
				case <-time.After(3 * time.Second):
					t.Fatal("BLPOP not reached")
				}
				time.Sleep(180 * time.Millisecond)
				count := calls.Load()
				if mode == "recovery" {
					deadline := time.Now().Add(2 * time.Second)
					for processor.processed.Load() == 0 && time.Now().Before(deadline) {
						time.Sleep(time.Millisecond)
					}
				}
				start := time.Now()
				switch b := broker.(type) {
				case *Broker:
					close(b.GetStopChan())
				case *BrokerGR:
					close(b.GetStopChan())
				}
				select {
				case e := <-done:
					if e != nil {
						t.Error(e)
					}
				case <-time.After(750 * time.Millisecond):
					t.Fatal("stop blocked during polling backoff")
				}
				if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
					t.Errorf("slow stop %v", elapsed)
				}
				switch b := broker.(type) {
				case *Broker:
					b.delayedWG.Wait()
					b.pool.Close()
				case *BrokerGR:
					b.delayedWG.Wait()
					b.rclient.Close()
				}
				if (mode == "error" || mode == "recovery") && count > 2 {
					t.Errorf("fast Redis errors caused %d BLPOP requests in 180ms", count)
				}
				if (mode == "empty" || mode == "task") && count < 4 {
					t.Errorf("ordinary polling was unnecessarily delayed: %d requests", count)
				}
				if (mode == "task" || mode == "recovery") && processor.processed.Load() != 1 {
					t.Errorf("processed %d tasks", processor.processed.Load())
				}
				t.Logf("BLPOP requests=%d, processed=%d", count, processor.processed.Load())
			})
		}
	}
}
