package redis

import (
	"context"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestShare(t *testing.T) {
	option := &redis.Options{
		Addr:     "localhost:6379",
		Username: "default",
		Password: "redispw",
	}
	rdb := redis.NewClient(option)

	key := "read-lock"
	readlock1 := NewReadLock(rdb, key)
	readlock2 := NewReadLock(rdb, key)

	_, _ = readlock1.LockWithTime(defaultTestExpire, defaultTestTimeout)
	_, _ = readlock2.LockWithTime(defaultTestExpire, defaultTestTimeout)

	mode := rdb.HGet(context.Background(), key, "mode").Val()
	assert.Equal(t, "read", mode)
	hlen := rdb.HLen(context.Background(), key).Val()
	assert.Equal(t, int64(3), hlen, "read lock has 2")
	keys := rdb.Keys(context.Background(), "read-lock:*:rlock_timeout").Val()
	assert.Equal(t, 2, len(keys), "read lock has 2")
	_ = readlock1.Unlock()
	hlen = rdb.HLen(context.Background(), key).Val()
	assert.Equal(t, int64(2), hlen, "")
	_ = readlock2.Unlock()
}

func TestRLockReentrant(t *testing.T) {
	option := &redis.Options{
		Addr:     "localhost:6379",
		Username: "default",
		Password: "redispw",
	}
	rdb := redis.NewClient(option)
	wait := sync.WaitGroup{}
	wait.Add(2)

	key := "read-lock-reentrant"
	readlock := NewReadLock(rdb, key)
	_, _ = readlock.LockWithTime(defaultTestExpire, defaultTestTimeout)
	mode := rdb.HGet(context.Background(), key, "mode").Val()
	assert.Equal(t, "read", mode)
	cnt, _ := rdb.HGet(context.Background(), key, readlock.id).Int()
	assert.Equal(t, 1, cnt)
	t.Run("reentrant-1", func(t *testing.T) {
		_, _ = readlock.LockWithTime(defaultTestExpire, defaultTestTimeout)
		cnt, _ := rdb.HGet(context.Background(), key, readlock.id).Int()
		assert.Equal(t, 2, cnt)
		t.Run("reentrant-2", func(t *testing.T) {
			_, _ = readlock.LockWithTime(defaultTestExpire, defaultTestTimeout)
			cnt, _ := rdb.HGet(context.Background(), key, readlock.id).Int()
			assert.Equal(t, 3, cnt)
			_ = readlock.Unlock()
			wait.Done()
		})
		_ = readlock.Unlock()
		wait.Done()
	})
	_ = readlock.Unlock()
}

func TestRLockTtl(t *testing.T) {
	option := &redis.Options{
		Addr:     "localhost:6379",
		Username: "default",
		Password: "redispw",
	}
	rdb := redis.NewClient(option)
	key := "read-lock-ttl"
	readlock1 := NewReadLock(rdb, key)
	readlock2 := NewReadLock(rdb, key)
	readlock3 := NewReadLock(rdb, key)
	timeoutKey1 := key + ":" + readlock1.id + ":rlock_timeout"
	timeoutKey2 := key + ":" + readlock2.id + ":rlock_timeout"
	timeoutKey3 := key + ":" + readlock3.id + ":rlock_timeout"

	_, _ = readlock1.LockWithTime(defaultTestExpire, defaultTestTimeout)
	mode := rdb.HGet(context.Background(), key, "mode").Val()
	assert.Equal(t, "read", mode)

	_, _ = readlock2.LockWithTime(defaultTestExpire/2, defaultTestTimeout/2)
	hKeyTtl := rdb.TTL(context.Background(), key).Val()
	keyTtl1 := rdb.TTL(context.Background(), timeoutKey1).Val()
	assert.Equal(t, hKeyTtl, keyTtl1)
	keyTtl2 := rdb.TTL(context.Background(), timeoutKey2).Val()
	assert.Greater(t, hKeyTtl, keyTtl2)

	// readlock1 reentrant
	_, _ = readlock3.LockWithTime(defaultTestExpire*2, defaultTestTimeout*2)
	hKeyTtl = rdb.TTL(context.Background(), key).Val()
	keyTtl1 = rdb.TTL(context.Background(), timeoutKey1).Val()
	keyTtl3 := rdb.TTL(context.Background(), timeoutKey3).Val()
	assert.Equal(t, hKeyTtl, keyTtl3)
	assert.Greater(t, hKeyTtl, keyTtl1)

	_ = readlock3.Unlock()
	hKeyTtl = rdb.TTL(context.Background(), key).Val()
	keyTtl1 = rdb.TTL(context.Background(), timeoutKey1).Val()
	assert.Equal(t, hKeyTtl, keyTtl1)

	_ = readlock2.Unlock()
	hKeyTtl = rdb.TTL(context.Background(), key).Val()
	keyTtl1 = rdb.TTL(context.Background(), timeoutKey1).Val()
	assert.Equal(t, hKeyTtl, keyTtl1)

	_ = readlock1.Unlock()
}
