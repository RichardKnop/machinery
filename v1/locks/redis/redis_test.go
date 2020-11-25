package redis

import (
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	lockiface "github.com/RichardKnop/machinery/v1/locks/iface"
	"github.com/RichardKnop/machinery/v1/utils"
	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
)

var (
	emptyConfig = &config.Config{
		Redis: &config.RedisConfig{},
	}
)

func TestLock_Lock(t *testing.T) {
	redisSrv, err := miniredis.Run()
	assert.NoError(t, err)
	defer redisSrv.Close()

	lock := New(emptyConfig, []string{redisSrv.Addr()}, 0, 3)
	keyName := utils.GetPureUUID()

	go func() {
		err := lock.Lock(keyName, time.Now().Add(25*time.Second).UnixNano())
		assert.NoError(t, err)
	}()
	time.Sleep(1 * time.Second)
	err = lock.Lock(keyName, time.Now().Add(25*time.Second).UnixNano())
	assert.Error(t, err)
	assert.EqualError(t, err, ErrRedisLockFailed.Error())
}

func TestLock_LockWithRetries(t *testing.T) {
	redisSrv, err := miniredis.Run()
	assert.NoError(t, err)
	defer redisSrv.Close()

	lock := New(emptyConfig, []string{redisSrv.Addr()}, 0, 3)
	keyName := utils.GetPureUUID()

	go func() {
		err := lock.LockWithRetries(keyName, time.Now().Add(25*time.Second).UnixNano())
		assert.NoError(t, err)
	}()
	time.Sleep(1 * time.Second)
	err = lock.LockWithRetries(keyName, time.Now().Add(25*time.Second).UnixNano())
	assert.Error(t, err)
	assert.EqualError(t, err, ErrRedisLockFailed.Error())
}

func TestNew(t *testing.T) {
	redisSrv, err := miniredis.Run()
	assert.NoError(t, err)
	defer redisSrv.Close()

	lock := New(emptyConfig, []string{redisSrv.Addr()}, 0, 3)
	assert.Implements(t, (*lockiface.Lock)(nil), lock)
}
