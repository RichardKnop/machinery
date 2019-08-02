package commonredis

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/gomodule/redigo/redis"
)

var (
	defaultConfig = &config.RedisConfig{
		MaxIdle:                3,
		IdleTimeout:            240,
		ReadTimeout:            15,
		WriteTimeout:           15,
		ConnectTimeout:         15,
		DelayedTasksPollPeriod: 20,
	}
)

// RedisConnector ...
type RedisConnector struct{}

// NewPool returns a new pool of Redis connections
func (rc *RedisConnector) NewPool(socketPath, host, password string, db int, cnf *config.RedisConfig, tlsConfig *tls.Config) *redis.Pool {
	if cnf == nil {
		cnf = defaultConfig
	}
	return &redis.Pool{
		MaxIdle:     cnf.MaxIdle,
		IdleTimeout: time.Duration(cnf.IdleTimeout) * time.Second,
		MaxActive:   cnf.MaxActive,
		Wait:        cnf.Wait,
		Dial: func() (redis.Conn, error) {
			c, err := rc.open(socketPath, host, password, db, cnf, tlsConfig)
			if err != nil {
				return nil, err
			}

			if db != 0 {
				_, err = c.Do("SELECT", db)
				if err != nil {
					return nil, err
				}
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
func (rc *RedisConnector) open(socketPath, host, password string, db int, cnf *config.RedisConfig, tlsConfig *tls.Config) (redis.Conn, error) {
	var opts = []redis.DialOption{
		redis.DialDatabase(db),
		redis.DialReadTimeout(time.Duration(cnf.ReadTimeout) * time.Second),
		redis.DialWriteTimeout(time.Duration(cnf.WriteTimeout) * time.Second),
		redis.DialConnectTimeout(time.Duration(cnf.ConnectTimeout) * time.Second),
	}

	if tlsConfig != nil {
		opts = append(opts, redis.DialTLSConfig(tlsConfig), redis.DialUseTLS(true))
	}

	if password != "" {
		opts = append(opts, redis.DialPassword(password))
	}

	if socketPath != "" {
		return redis.Dial("unix", socketPath, opts...)
	}

	return redis.Dial("tcp", host, opts...)
}

// ParseRedisURL ...
func ParseRedisURL(urlStr string) (host, password string, db int, err error) {
	// redis://pwd@host/db

	var u *url.URL
	u, err = url.Parse(urlStr)
	if err != nil {
		return
	}
	if u.Scheme != "redis" {
		err = errors.New("No redis scheme found")
		return
	}

	if u.User != nil {
		var exists bool
		password, exists = u.User.Password()
		if !exists {
			password = u.User.Username()
		}
	}

	host = u.Host

	parts := strings.Split(u.Path, "/")
	if len(parts) == 1 {
		db = 0 //default redis db
	} else {
		db, err = strconv.Atoi(parts[1])
		if err != nil {
			db, err = 0, nil //ignore err here
		}
	}

	return
}

// ParseRedisSocketURL extracts Redis connection options from a URL with the
// redis+socket:// scheme. This scheme is not standard (or even de facto) and
// is used as a transitional mechanism until the the config package gains the
// proper facilities to support socket-based connections.
func ParseRedisSocketURL(url string) (path, password string, db int, err error) {
	parts := strings.Split(url, "redis+socket://")
	if parts[0] != "" {
		err = errors.New("No redis scheme found")
		return
	}

	// redis+socket://password@/path/to/file.soc:/db

	if len(parts) != 2 {
		err = fmt.Errorf("Redis socket connection string should be in format redis+socket://password@/path/to/file.sock:/db, instead got %s", url)
		return
	}

	remainder := parts[1]

	// Extract password if any
	parts = strings.SplitN(remainder, "@", 2)
	if len(parts) == 2 {
		password = parts[0]
		remainder = parts[1]
	} else {
		remainder = parts[0]
	}

	// Extract path
	parts = strings.SplitN(remainder, ":", 2)
	path = parts[0]
	if path == "" {
		err = fmt.Errorf("Redis socket connection string should be in format redis+socket://password@/path/to/file.sock:/db, instead got %s", url)
		return
	}
	if len(parts) == 2 {
		remainder = parts[1]
	}

	// Extract DB if any
	parts = strings.SplitN(remainder, "/", 2)
	if len(parts) == 2 {
		db, _ = strconv.Atoi(parts[1])
	}

	return
}
