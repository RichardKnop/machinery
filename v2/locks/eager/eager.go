package eager

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrEagerLockFailed = errors.New("eager lock: failed to acquire lock")
)

type Lock struct {
	retries  int
	interval time.Duration
	register struct {
		sync.RWMutex
		m map[string]int64
	}
}

func New() *Lock {
	return &Lock{
		retries:  3,
		interval: 5 * time.Second,
		register: struct {
			sync.RWMutex
			m map[string]int64
		}{m: make(map[string]int64)},
	}
}

func (e *Lock) LockWithRetries(key string, value int64) error {
	for i := 0; i <= e.retries; i++ {
		err := e.Lock(key, value)
		if err == nil {
			//成功拿到锁，返回
			return nil
		}

		time.Sleep(e.interval)
	}
	return ErrEagerLockFailed
}

func (e *Lock) Lock(key string, value int64) error {
	e.register.RLock()
	defer e.register.RUnlock()
	timeout, exist := e.register.m[key]
	if !exist || time.Now().UnixNano() > timeout {
		e.register.m[key] = value
		return nil
	}
	return ErrEagerLockFailed
}
