package redis

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v9"
	"github.com/google/uuid"
	"time"
)

var (
	writeLock = redis.NewScript("local mode = redis.call('hget', KEYS[1], 'mode');\n" +
		"    local writeThreadKey =  ARGV[2] .. ':write'\n" +
		"    if mode == false then\n" +
		"        redis.call('hset', KEYS[1], 'mode', 'write');\n" +
		"        redis.call('hincrby', KEYS[1], writeThreadKey, 1);\n" +
		"        redis.call('pexpire', KEYS[1], ARGV[1]);\n" +
		"        return 0;\n" +
		"    end;\n" +
		"    if mode == 'write' and redis.call('hexists', KEYS[1], writeThreadKey) == 1 then\n" +
		"        redis.call('hincrby', KEYS[1], writeThreadKey, 1);\n" +
		"        redis.call('pexpire', KEYS[1], ARGV[1]);\n" +
		"        return 0;\n" +
		"    end;\n" +
		"    return redis.call('pttl', KEYS[1]);")
	writeUnlock = redis.NewScript("local mode = redis.call('hget', KEYS[1], 'mode');\n" +
		"    if mode == false then\n" +
		"        return 1;\n" +
		"    end;\n" +
		"    if mode == 'write' then\n" +
		"        local writeThreadKey =  ARGV[2] .. ':write'\n" +
		"        local lockExists = redis.call('hexists', KEYS[1], writeThreadKey)\n" +
		"        if lockExists == 0 then\n" +
		"            return 0;\n" +
		"        else\n" +
		"            local counter = redis.call('hincrby', KEYS[1], writeThreadKey, -1);\n" +
		"            if counter > 0 then\n" +
		"                redis.call('pexpire', KEYS[1], ARGV[1]);\n" +
		"                return 0;\n" +
		"            else\n" +
		"                redis.call('hdel', KEYS[1], writeThreadKey);\n" +
		"                if (redis.call('hlen', KEYS[1]) == 1) then\n" +
		"                    redis.call('del', KEYS[1]);\n" +
		"                else\n" +
		"                    redis.call('hset', KEYS[1], 'mode', 'read');\n" +
		"                end;\n" +
		"                return 1;\n" +
		"            end;\n" +
		"        end;\n" +
		"    end;\n" +
		"    return 0;")
)

type WriteLock struct {
	rdb     *redis.Client
	ttl     time.Duration
	id      string
	key     string
	expire  time.Duration
	timeout time.Duration
}

func NewWriteLock(rdb *redis.Client, key string) *WriteLock {
	return &WriteLock{
		rdb:     rdb,
		id:      uuid.NewString(),
		key:     key,
		expire:  defaultExpire,
		timeout: defaultTimeout,
	}
}

func (lock *WriteLock) Lock() (bool, error) {
	return lock.tryLock(context.Background())
}

func (lock *WriteLock) LockWithTime(expire, timeout time.Duration) (bool, error) {
	lock.expire = expire
	lock.timeout = timeout
	return lock.tryLock(context.Background())
}

func (lock *WriteLock) Unlock() error {
	_, err := writeUnlock.Run(context.Background(),
		lock.rdb, []string{lock.key}, lock.expire.Milliseconds(), lock.id).Result()
	if err != nil {
		return err
	}
	return nil
}

func (lock *WriteLock) tryLock(ctx context.Context) (bool, error) {
	waitTimeCtx, cancel := context.WithDeadline(ctx, time.Now().Add(lock.timeout))
	defer cancel()

	var timer *time.Timer
	for {
		ttl, err := writeLock.Run(ctx, lock.rdb, []string{lock.key}, lock.expire.Milliseconds(), lock.id).Int()
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
