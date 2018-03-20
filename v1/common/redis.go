package common

import (
	"time"

	"github.com/gomodule/redigo/redis"
)

// RedisConnector ...
type RedisConnector struct{}

// NewPool returns a new pool of Redis connections
func (rc *RedisConnector) NewPool(socketPath, host, password string, db int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := rc.open(socketPath, host, password, db)
			if err != nil {
				return nil, err
			}

			if db != 0 {
				_, err = c.Do("SELECT", db)
			}

			if err != nil {
				return nil, err
			}
			return c, err
		},
		// PINGs connections that have been idle more than 10 seconds
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Duration(10*time.Second) {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

// Open a new Redis connection
func (rc *RedisConnector) open(socketPath, host, password string, db int) (redis.Conn, error) {
	var opts = []redis.DialOption{
		redis.DialDatabase(db),
		redis.DialReadTimeout(15 * time.Second),
		redis.DialWriteTimeout(15 * time.Second),
		redis.DialConnectTimeout(15 * time.Second),
	}

	if password != "" {
		opts = append(opts, redis.DialPassword(password))
	}

	if socketPath != "" {
		return redis.Dial("unix", socketPath, opts...)
	}

	return redis.Dial("tcp", host, opts...)
}
