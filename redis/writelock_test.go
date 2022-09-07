package redis

import (
	"context"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWriteLock(t *testing.T) {
	option := &redis.Options{
		Addr:     "localhost:49153",
		Username: "default",
		Password: "redispw",
	}
	rdb := redis.NewClient(option)

	key := "write-lock"
	writeLock := NewWriteLock(rdb, key)

	_, _ = writeLock.LockWithTime(defaultTestExpire, defaultTestTimeout)
	mode := rdb.HGet(context.Background(), key, "mode").Val()
	assert.Equal(t, "write", mode)

	_ = writeLock.Unlock()

}

func TestName(t *testing.T) {
	option := &redis.Options{
		Addr:     "localhost:49153",
		Username: "default",
		Password: "redispw",
	}
	rdb := redis.NewClient(option)

	key := "read-write-lock"
	readLock := NewReadLock(rdb, key)
	writeLock := NewWriteLock(rdb, key)
	writeLock.id = readLock.id
	_, _ = writeLock.LockWithTime(defaultTestExpire, defaultTestTimeout)
	_, _ = readLock.LockWithTime(defaultTestExpire, defaultTestTimeout)
	mode := rdb.HGet(context.Background(), key, "mode").Val()
	assert.Equal(t, "write", mode)
	hLen := rdb.HLen(context.Background(), key).Val()
	assert.Equal(t, int64(3), hLen)

	_ = writeLock.Unlock()
	mode = rdb.HGet(context.Background(), key, "mode").Val()
	assert.Equal(t, "read", mode)
	hLen = rdb.HLen(context.Background(), key).Val()
	assert.Equal(t, int64(2), hLen)

	_ = readLock.Unlock()
}
