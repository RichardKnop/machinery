package eager

import (
	lockiface "github.com/RichardKnop/machinery/v1/locks/iface"
	"github.com/RichardKnop/machinery/v1/utils"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLock_Lock(t *testing.T) {
	lock := New()
	keyName := utils.GetPureUUID()

	go func() {
		err := lock.Lock(keyName, time.Now().Add(25*time.Second).UnixNano())
		assert.NoError(t, err)
	}()
	time.Sleep(1 * time.Second)
	err := lock.Lock(keyName, time.Now().Add(25*time.Second).UnixNano())
	assert.Error(t, err)
	assert.EqualError(t, err, ErrEagerLockFailed.Error())
}

func TestLock_LockWithRetries(t *testing.T) {
	lock := New()
	keyName := utils.GetPureUUID()

	go func() {
		err := lock.LockWithRetries(keyName, time.Now().Add(25*time.Second).UnixNano())
		assert.NoError(t, err)
	}()
	time.Sleep(1 * time.Second)
	err := lock.LockWithRetries(keyName, time.Now().Add(25*time.Second).UnixNano())
	assert.Error(t, err)
	assert.EqualError(t, err, ErrEagerLockFailed.Error())
}

func TestNew(t *testing.T) {
	lock := New()
	assert.Implements(t, (*lockiface.Lock)(nil), lock)
}
