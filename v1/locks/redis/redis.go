package redis

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/go-redis/redis/v8"
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

	i := strings.LastIndex(addrs[0], "@")
	if i > 0 {
		// with passwrod
		password = addrs[0][i+1:]
		addrs[0] = addrs[0][:i]
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

	ctx := r.rclient.Context()

	success, err := r.rclient.SetNX(ctx, key, value, time.Duration(value+1)).Result()
	if err != nil {
		return err
	}

	if !success {
		v, err := r.rclient.Get(ctx, key).Result()
		if err != nil {
			return err
		}
		timeout, err := strconv.Atoi(v)
		if err != nil {
			return err
		}

		if timeout != 0 && now > int64(timeout) {
			newTimeout, err := r.rclient.GetSet(ctx, key, value).Result()
			if err != nil {
				return err
			}

			curTimeout, err := strconv.Atoi(newTimeout)
			if err != nil {
				return err
			}

			if now > int64(curTimeout) {
				// success to acquire lock with get set
				// set the expiration of redis key
				r.rclient.Expire(ctx, key, time.Duration(value+1))
				return nil
			}

			return ErrRedisLockFailed
		}

		return ErrRedisLockFailed
	}

	return nil
}
