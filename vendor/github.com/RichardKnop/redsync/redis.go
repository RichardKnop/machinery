package redsync

import "github.com/gomodule/redigo/redis"

// A Pool maintains a pool of Redis connections.
type Pool interface {
	Get() redis.Conn
}
