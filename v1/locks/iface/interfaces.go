package iface

type Lock interface {
	//带重试的获取锁, key为锁的名字，锁需要在value（纳秒时间戳）时刻自动释放
	LockWithRetries(key string, value int64) error

	//获取锁, key为锁的名字，锁需要在value（纳秒时间戳）时刻自动释放
	Lock(key string, value int64) error
}
