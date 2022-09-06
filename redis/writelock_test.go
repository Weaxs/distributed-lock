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
