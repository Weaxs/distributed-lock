package redis

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v9"
	"github.com/google/uuid"
	"time"
)

var (
	hashLock = redis.NewScript("if (redis.call('exists', KEYS[1]) == 0) then\n" +
		"    redis.call('hset', KEYS[1], ARGV[2], 1);\n" +
		"    redis.call('pexpire', KEYS[1], ARGV[1]);\n" +
		"    return 0;\n    " +
		"end;\n" +
		"if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then\n" +
		"    redis.call('hincrby', KEYS[1], ARGV[2], 1);\n" +
		"    redis.call('pexpire', KEYS[1], ARGV[1]);\n" +
		"    return 0;\n    " +
		"end;\n" +
		"return redis.call('pttl', KEYS[1]);")

	hashUnlock = redis.NewScript("if (redis.call('hexists', KEYS[1], ARGV[2]) == 0) then\n" +
		"    return 0;\n" +
		"end;\n" +
		"local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1)\n" +
		"if (counter > 0) then\n" +
		"    redis.call('pexpire', KEYS[1], ARGV[1]);\n" +
		"    return 1;\n" +
		"else\n" +
		"    redis.call('del', KEYS[1]);\n" +
		"    return 1;\n" +
		"end;\n" +
		"return 0;")
)

type HashLock struct {
	rdb     *redis.Client
	ttl     time.Duration
	id      string
	key     string
	expire  time.Duration
	timeout time.Duration
}

func NewHashLock(rdb *redis.Client, key string) *HashLock {
	return &HashLock{
		rdb:     rdb,
		id:      uuid.NewString(),
		key:     key,
		expire:  defaultExpire,
		timeout: defaultTimeout,
	}
}

func (lock *HashLock) Lock() (bool, error) {
	return lock.tryLock(context.Background())
}

func (lock *HashLock) LockWithTime(expire, timeout time.Duration) (bool, error) {
	lock.expire = expire
	lock.timeout = timeout
	return lock.tryLock(context.Background())
}

func (lock *HashLock) Unlock() error {
	_, err := hashUnlock.Run(context.Background(),
		lock.rdb, []string{lock.key}, lock.expire.Milliseconds(), lock.id).Result()
	if err != nil {
		return err
	}
	return nil
}

func (lock *HashLock) tryLock(ctx context.Context) (bool, error) {
	waitTimeCtx, cancel := context.WithDeadline(ctx, time.Now().Add(lock.timeout))
	defer cancel()

	var timer *time.Timer
	for {
		ttl, err := hashLock.Run(ctx, lock.rdb, []string{lock.key}, lock.expire.Milliseconds(), lock.id).Int()
		if err != nil {
			return false, err
		} else if ttl == 0 {
			return true, nil
		}

		var sleepTime time.Duration
		if ttl < 100 {
			sleepTime = time.Duration(ttl) * time.Millisecond
		} else {
			sleepTime = time.Duration(ttl/3) * time.Millisecond
		}
		if timer == nil {
			timer = time.NewTimer(sleepTime)
			defer timer.Stop()
		} else {
			timer.Reset(sleepTime)
		}

		select {
		case <-waitTimeCtx.Done():
			lock.ttl = time.Duration(ttl) * time.Millisecond
			return false, errors.New("waiting time out")
		case <-timer.C:
		}
	}
}
