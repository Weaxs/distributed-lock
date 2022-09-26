/*
 * Copyright(C) 2022 Baidu Inc. All Rights Reserved.
 * Author: Zhang YiKun (zhangyikun01@baidu.com)
 * Date: 2022/8/30
 */

package redis

import (
	"context"
	"errors"
	"github.com/Weaxs/distributed-lock/common"
	"github.com/go-redis/redis/v9"
	"github.com/google/uuid"
	"time"
)

var (
	readLock = redis.NewScript("local mode = redis.call('hget', KEYS[1], 'mode');\n" +
		"    if mode == false then\n" +
		"        redis.call('hset', KEYS[1], 'mode', 'read');\n" +
		"        redis.call('hincrby', KEYS[1], ARGV[2], 1);\n" +
		"        local timeoutKey = KEYS[1] .. ':' .. ARGV[2] .. ':rlock_timeout'\n" +
		"        redis.call('incr', timeoutKey);\n" +
		"        redis.call('pexpire', timeoutKey, ARGV[1]);\n" +
		"        redis.call('pexpire', KEYS[1], ARGV[1]);\n" +
		"        return 0;\n" +
		"    end;\n" +
		"    if (mode == 'read' or (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[2] .. ':write'))) then\n" +
		"        redis.call('hincrby', KEYS[1], ARGV[2], 1);\n" +
		"        local timeoutKey = KEYS[1] .. ':' .. ARGV[2] .. ':rlock_timeout'\n" +
		"        redis.call('incr', timeoutKey);\n" +
		"        redis.call('pexpire', timeoutKey, ARGV[1]);\n" +
		"        local remainTime = redis.call('pttl', KEYS[1]);\n" +
		"        redis.call('pexpire', KEYS[1], math.max(remainTime, ARGV[1]));\n" +
		"        return 0;\n" +
		"    end;\n" +
		"    return redis.call('pttl', KEYS[1]);")
	readUnlock = redis.NewScript("local mode = redis.call('hget', KEYS[1], 'mode');\n" +
		"    if mode == false then\n" +
		"        return 1;\n" +
		"    end;\n" +
		"    if redis.call('hexists', KEYS[1], ARGV[2]) == 0 then\n" +
		"        return 1;\n" +
		"    end;\n" +
		"    local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1);\n" +
		"    local timeoutKey = KEYS[1] .. ':' .. ARGV[2] .. ':rlock_timeout'\n" +
		"    redis.call('decr', timeoutKey)\n    if counter == 0 then\n" +
		"        redis.call('hdel', KEYS[1], ARGV[2]);\n" +
		"        redis.call('del', timeoutKey);\n" +
		"    end;\n" +
		"    if (redis.call('hlen', KEYS[1]) > 1) then\n" +
		"        if mode == 'write' then\n" +
		"            return 0;\n" +
		"        end;\n" +
		"        local maxRemainTime = -1;\n" +
		"        local keys = redis.call('hkeys', KEYS[1]);\n" +
		"        for n, key in ipairs(keys) do\n" +
		"            local otherTimeoutKey = KEYS[1] .. ':' .. key .. ':rlock_timeout';\n " +
		"           local remainTime = redis.call('pttl', otherTimeoutKey);\n" +
		"            maxRemainTime = math.max(remainTime, maxRemainTime);\n" +
		"        end;\n" +
		"        if maxRemainTime > 0 then\n" +
		"            redis.call('pexpire', KEYS[1], maxRemainTime);\n" +
		"            return 0\n" +
		"        end;\n" +
		"    end;\n" +
		"    redis.call('del', KEYS[1]);\n" +
		"    return 1")
)

type ReadLock struct {
	rdb     *redis.Client
	ttl     time.Duration
	id      string
	key     string
	expire  time.Duration
	timeout time.Duration
}

func NewReadLock(rdb *redis.Client, key string) *ReadLock {
	return &ReadLock{
		rdb:     rdb,
		id:      uuid.NewString(),
		key:     key,
		expire:  common.DefaultExpire,
		timeout: common.DefaultTimeout,
	}
}

func (lock *ReadLock) Lock() (bool, error) {
	return lock.tryLock(context.Background())
}

func (lock *ReadLock) LockWithTime(expire, timeout time.Duration) (bool, error) {
	lock.expire = expire
	lock.timeout = timeout
	return lock.tryLock(context.Background())
}

func (lock *ReadLock) Unlock() error {
	_, err := readUnlock.Run(context.Background(),
		lock.rdb, []string{lock.key}, lock.expire.Milliseconds(), lock.id).Result()
	if err != nil {
		return err
	}
	return nil
}

func (lock *ReadLock) tryLock(ctx context.Context) (bool, error) {
	waitTimeCtx, cancel := context.WithDeadline(ctx, time.Now().Add(lock.timeout))
	defer cancel()

	var timer *time.Timer
	for {
		ttl, err := readLock.Run(ctx, lock.rdb, []string{lock.key}, lock.expire.Milliseconds(), lock.id).Int()
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
