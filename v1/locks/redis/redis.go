package redis

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/go-redis/redis"
)

var (
	ErrRedisLockFailed = errors.New("redis lock: failed to acquire lock")
)

type Lock struct {
	rclient  redis.UniversalClient
	retries  int
	interval time.Duration
}

func New(cnf *config.Config, addrs []string, db, retries int) Lock {
	if retries <= 0 {
		return Lock{}
	}
	lock := Lock{retries: retries}

	var password string
	parts := strings.Split(addrs[0], "@")
	if len(parts) == 2 {
		// with passwrod
		password = parts[0]
		addrs[0] = parts[1]
	}

	ropt := &redis.UniversalOptions{
		Addrs:    addrs,
		DB:       db,
		Password: password,
	}
	if cnf.Redis != nil {
		ropt.MasterName = cnf.Redis.MasterName
	}

	lock.rclient = redis.NewUniversalClient(ropt)

	return lock
}

//try lock with retries
func (r Lock) LockWithRetries(key string, value int64) error {
	for i := 0; i <= r.retries; i++ {
		err := r.Lock(key, value)
		if err == nil {
			//成功拿到锁，返回
			return nil
		}

		time.Sleep(r.interval)
	}
	return ErrRedisLockFailed
}

func (r Lock) Lock(key string, value int64) error {
	var now = time.Now().UnixNano()

	success, err := r.rclient.SetNX(key, value, 0).Result()
	if err != nil {
		return err
	}

	if !success {
		v, err := r.rclient.Get(key).Result()
		if err != nil {
			return err
		}
		timeout, err := strconv.Atoi(v)
		if err != nil {
			return err
		}

		if timeout != 0 && now > int64(timeout) {
			newTimeout, err := r.rclient.GetSet(key, value).Result()
			if err != nil {
				return err
			}

			curTimeout, err := strconv.Atoi(newTimeout)
			if err != nil {
				return err
			}

			if now > int64(curTimeout) {
				//使用getset加锁成功
				return nil
			}

			return ErrRedisLockFailed
		}

		return ErrRedisLockFailed
	}

	return nil
}
