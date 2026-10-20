package redis

import (
	"bufio"
	"context"
	"errors"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/tasks"
	sdk "github.com/redis/go-redis/v9"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestContextCredentials(t *testing.T) {
	for _, mode := range []string{"static", "provider", "provider-error"} {
		t.Run(mode, func(t *testing.T) {
			listener, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			var wg sync.WaitGroup
			var mu sync.Mutex
			connections := []net.Conn{}
			var authorized atomic.Int32
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
						reader := bufio.NewReader(conn)
						for {
							line, e := reader.ReadString('\n')
							if e != nil {
								return
							}
							n, e := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
							if e != nil {
								return
							}
							parts := make([]string, n)
							for i := range parts {
								line, e = reader.ReadString('\n')
								if e != nil {
									return
								}
								size, e := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "$")))
								if e != nil {
									return
								}
								buf := make([]byte, size+2)
								if _, e = io.ReadFull(reader, buf); e != nil {
									return
								}
								parts[i] = string(buf[:size])
							}
							reply := "+OK\r\n"
							switch strings.ToUpper(parts[0]) {
							case "HELLO":
								reply = "-ERR unknown command 'hello'\r\n"
							case "AUTH":
								user, password := "static-user", "static-password"
								if mode == "provider" {
									user, password = "callback-user", "callback-password"
								}
								if len(parts) == 3 && parts[1] == user && parts[2] == password {
									authorized.Add(1)
								} else {
									reply = "-WRONGPASS synthetic authentication failed\r\n"
								}
							case "RPUSH":
								reply = ":1\r\n"
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
			cfg := &config.Config{DefaultQueue: "credential-test", Redis: &config.RedisConfig{}}
			failure := errors.New("synthetic credential provider failure")
			var called atomic.Int32
			if mode != "static" {
				cfg.Redis.CredentialsProviderContext = func(ctx context.Context) (string, string, error) {
					called.Add(1)
					if ctx == nil {
						return "", "", errors.New("missing context")
					}
					if mode == "provider-error" {
						return "", "", failure
					}
					return "callback-user", "callback-password", nil
				}
			}
			b := NewGR(cfg, []string{"static-user:static-password@" + listener.Addr().String()}, 0).(*BrokerGR)
			defer b.rclient.Close()
			e = b.Publish(context.Background(), &tasks.Signature{UUID: "credential-task", Name: "credential-task"})
			if mode == "provider-error" {
				if !errors.Is(e, failure) {
					t.Errorf("provider error not propagated: %v", e)
				}
				if authorized.Load() != 0 {
					t.Error("authenticated despite provider failure")
				}
			} else if e != nil {
				t.Error(e)
			} else if authorized.Load() == 0 {
				t.Error("no successful authentication")
			}
			if mode != "static" && called.Load() == 0 {
				t.Error("provider was not called")
			}
		})
	}
}

func TestClusterContextCredentialsOption(t *testing.T) {
	cfg := &config.Config{Redis: &config.RedisConfig{ClusterEnabled: true, CredentialsProviderContext: func(context.Context) (string, string, error) { return "", "", nil }}}
	b := NewGR(cfg, []string{"127.0.0.1:1"}, 0).(*BrokerGR)
	defer b.rclient.Close()
	client, ok := b.rclient.(*sdk.ClusterClient)
	if !ok {
		t.Fatal("not a cluster client")
	}
	if client.Options().CredentialsProviderContext == nil {
		t.Error("cluster client lost provider")
	}
}
