
-- lock struct:
-- lock_key : { thread_key : reentrant_count }       expire: ARGV[1]

local redis = require("redis")

-- KEYS[1] lock key
-- ARGV[1] expire time (unit: ms)
-- ARGV[2] global unique threadId
function lock(KEYS, ARGV)
    -- first lock
    if (redis.call('exists', KEYS[1]) == 0) then
        redis.call('hincrby', KEYS[1], ARGV[2], 1);
        redis.call('pexpire', KEYS[1], ARGV[1]);
        return nil;
    end;
    -- reentrant
    if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
        redis.call('hincrby', KEYS[1], ARGV[2], 1);
        redis.call('pexpire', KEYS[1], ARGV[1]);
        return nil;
    end;
    -- lock faile
    return redis.call('pttl', KEYS[1]);
end

-- KEYS[1] lock key
-- ARGV[1] expire time (unit: ms)
-- ARGV[2] global unique threadId
function unlock(KEYS, ARGV)
    if (redis.call('hexists', KEYS[1], ARGV[2]) == 0) then
        return nil;
    end;
    local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1)
    if (counter > 0) then
        redis.call('pexpire', KEYS[1], ARGV[1]);
        return 0;
    else
        redis.call('del', KEYS[1]);
        return 1;
    end;
    return nil;
end
