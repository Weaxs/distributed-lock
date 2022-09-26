package redis

import (
	"context"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

const (
	defaultTestExpire  = 1 * time.Minute
	defaultTestTimeout = 2 * time.Minute
)

func TestReentrant(t *testing.T) {
	option := &redis.Options{
		Addr:     "localhost:6379",
		Username: "default",
		Password: "redispw",
	}
	rdb := redis.NewClient(option)
	defer func(rdb *redis.Client) {
		_ = rdb.Close()
	}(rdb)
	wait := sync.WaitGroup{}
	wait.Add(2)

	key := "reentrant-lock"
	hashLock := NewHashLock(rdb, key)

	err := lockTest(t, hashLock, defaultTestExpire, defaultTestTimeout)
	reentrant, _ := rdb.HGet(context.Background(), hashLock.key, hashLock.id).Int()
	assert.Equal(t, 1, reentrant, "reentrant count should be 1")
	if err != nil {
		return
	}
	t.Run("reentrant-1", func(t *testing.T) {
		t.Log("reentrant lock start, thead: " + t.Name())
		_ = lockTest(t, hashLock, defaultTestExpire, defaultTestTimeout)
		reentrant, _ := rdb.HGet(context.Background(), hashLock.key, hashLock.id).Int()
		assert.Equal(t, 2, reentrant, "reentrant count should be 2")
		t.Run("reentrant-1-1", func(t *testing.T) {
			_ = lockTest(t, hashLock, defaultTestExpire, defaultTestTimeout)
			reentrant, _ := rdb.HGet(context.Background(), hashLock.key, hashLock.id).Int()
			assert.Equal(t, 3, reentrant, "reentrant count should be 3")
			unlockTest(t, hashLock)
			wait.Done()
		})
		unlockTest(t, hashLock)
		wait.Done()

	})
	wait.Wait()
	unlockTest(t, hashLock)
}

func TestHLockFailed(t *testing.T) {
	option := &redis.Options{
		Addr:     "localhost:6379",
		Username: "default",
		Password: "redispw",
	}
	rdb := redis.NewClient(option)
	defer func(rdb *redis.Client) {
		_ = rdb.Close()
	}(rdb)
	wait := sync.WaitGroup{}
	wait.Add(2)
	lockName := "hashLock"
	t.Run("lock-1", func(t *testing.T) {
		hashLock := NewHashLock(rdb, lockName)
		_ = lockTest(t, hashLock, defaultTestExpire, defaultTestTimeout)
		//unlockTest(t, hashLock)
		wait.Done()
	})

	t.Run("lock-2", func(t *testing.T) {
		hashLock := NewHashLock(rdb, lockName)
		_ = lockTest(t, hashLock, defaultTestExpire/4, defaultTestTimeout/4)
		unlockTest(t, hashLock)
		wait.Done()
	})

	wait.Wait()

}

func lockTest(t *testing.T, hashLock *HashLock, expire, timeout time.Duration) error {
	t.Log("lock start, thead: " + t.Name())
	_, err := hashLock.LockWithTime(expire, timeout)
	if err != nil {
		t.Log("Acquire lock failed, err: " + err.Error())
		t.Log("lock(" + hashLock.key + ") ttl: " + hashLock.ttl.String())
		t.Fail()
		return err
	}
	t.Log("Acquire lock success")
	return nil
}

func unlockTest(t *testing.T, hashLock *HashLock) {
	_ = hashLock.Unlock()
	t.Log(t.Name() + " lock end")
}
