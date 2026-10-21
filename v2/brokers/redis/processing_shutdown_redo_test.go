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

type shutdownProcessor struct{ processed atomic.Int32 }

func (p *shutdownProcessor) Process(*tasks.Signature) error {
	p.processed.Add(1)
	return fmt.Errorf("processor persistence failure")
}
func (*shutdownProcessor) CustomQueue() string     { return "" }
func (*shutdownProcessor) PreConsumeHandler() bool { return true }

func TestRedisProcessingErrorStopsProducers(t *testing.T) {
	for _, driver := range []string{"redigo"} {
		for _, mode := range []string{"task"} {
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
				processor := new(shutdownProcessor)
				done := make(chan error, 1)
				go func() { _, e := broker.StartConsuming("poll", 1, processor); done <- e }()
				select {
				case <-first:
				case e := <-done:
					t.Fatalf("consumer ended before BLPOP: %v", e)
				case <-time.After(3 * time.Second):
					t.Fatal("BLPOP not reached")
				}

				select {
				case e := <-done:
					if e == nil {
						t.Error("processing failure lost")
					}
				case <-time.After(time.Second):
					t.Error("processing error did not stop producers")
					close(broker.(*Broker).GetStopChan())
					select {
					case <-done:
					case <-time.After(2 * time.Second):
						t.Fatal("fallback stop failed")
					}
				}
				b := broker.(*Broker)
				b.delayedWG.Wait()
				b.processingWG.Wait()
				b.pool.Close()
				if processor.processed.Load() != 1 {
					t.Errorf("processed=%d", processor.processed.Load())
				}
			})
		}
	}
}
