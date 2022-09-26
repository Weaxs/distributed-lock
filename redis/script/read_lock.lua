
-- lock struct:
-- lock_key : { thread_key : reentrant_count, "mode" : "read"  }       expire: ARGV[1]          Hash
-- (lock_key:thread_key:suffix) : reentrant_count       expire: ARGV[1]          String
-- String struct for setting timeout key
-- reentrant use one timeout key

local redis = require("redis")

-- KEYS[1] lock key
-- ARGV[1] expire time (unit: ms)
-- ARGV[2] thread_key
-- ARGV[3] timeoutKey suffix
function lock(KEYS, ARGV)
    local mode = redis.call('hget', KEYS[1], 'mode');
    if mode == false then
        redis.call('hset', KEYS[1], 'mode', 'read');
        redis.call('hincrby', KEYS[1], ARGV[2], 1);
        local timeoutKey = KEYS[1] .. ':' .. ARGV[2] .. ':' .. ARGV[3]
        redis.call('incr', timeoutKey);
        redis.call('pexpire', timeoutKey, ARGV[1]);
        redis.call('pexpire', KEYS[1], ARGV[1]);
        return 0;
    end;
    if (mode == 'read' or (mode == 'write' and redis.call('hexists', KEYS[1], ARGV[2] .. ':write'))) then
        redis.call('hincrby', KEYS[1], ARGV[2], 1);
        local timeoutKey = KEYS[1] .. ':' .. ARGV[2] .. ':' .. ARGV[3]
        redis.call('incr', timeoutKey);
        redis.call('pexpire', timeoutKey, ARGV[1]);
        local remainTime = redis.call('pttl', KEYS[1]);
        redis.call('pexpire', KEYS[1], math.max(remainTime, ARGV[1]));
        return 0;
    end;
    return redis.call('pttl', KEYS[1]);
end

-- KEYS[1] lock key
-- ARGV[1] expire time (unit: ms)
-- ARGV[2] thread_key
-- ARGV[3] timeoutKey suffix
function unlock(KEYS, ARGV)
    local mode = redis.call('hget', KEYS[1], 'mode');
    if mode == false then
        return 1;
    end;
    if redis.call('hexists', KEYS[1], ARGV[2]) == 0 then
        return 1;
    end;

    local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1);
    local timeoutKey = KEYS[1] .. ':' .. ARGV[2] .. ':' .. ARGV[3]
    redis.call('decr', timeoutKey)
    if counter == 0 then
        redis.call('hdel', KEYS[1], ARGV[2]);
        redis.call('del', timeoutKey);
    end;
    if (redis.call('hlen', KEYS[1]) > 1) then
        -- write lock reentrant read lock
        if mode == 'write' then
            return 0;
        end;
        -- read lock
        local maxRemainTime = -1;
        local keys = redis.call('hkeys', KEYS[1]);
        for n, key in ipairs(keys) do
            local otherTimeoutKey = KEYS[1] .. ':' .. key .. ':' .. ARGV[3];
            local remainTime = redis.call('pttl', otherTimeoutKey);
            maxRemainTime = math.max(remainTime, maxRemainTime);
        end;
        -- renew expire
        if maxRemainTime > 0 then
            redis.call('pexpire', KEYS[1], maxRemainTime);
            return 0
        end;
    end;
    -- have no other thead
    redis.call('del', KEYS[1]);
    return 1
end