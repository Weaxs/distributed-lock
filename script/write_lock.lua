
-- lock_key : { (thread_key:write) : reentrant_count, "mode" : "read"  }       expire: ARGV[1]          Hash

local redis = require("redis")

-- KEYS[1] lock key
-- ARGV[1] expire time
-- ARGV[2] thread key
function lock(KEYS, ARGV)
    local mode = redis.call('hget', KEYS[1], 'mode');
    local writeThreadKey =  ARGV[2] .. ':write'
    if mode == false then
        redis.call('hset', KEYS[1], 'mode', 'write');
        redis.call('hincrby', KEYS[1], writeThreadKey, 1);
        redis.call('pexpire', KEYS[1], ARGV[1]);
        return 0;
    end;
    if mode == 'write' and redis.call('hexists', KEYS[1], writeThreadKey) == 1 then
        redis.call('hincrby', KEYS[1], writeThreadKey, 1);
        redis.call('pexpire', KEYS[1], ARGV[1]);
        return 0;
    end;
    return redis.call('pttl', KEYS[1]);
end

-- KEYS[1] lock key
-- ARGV[1] expire time
-- ARGV[2] thread key
function unlock(KEYS, ARGV)
    local mode = redis.call('hget', KEYS[1], 'mode');
    if mode == false then
        return 1;
    end;
    if mode == 'write' then
        local writeThreadKey =  ARGV[2] .. ':write'
        local lockExists = redis.call('hexists', KEYS[1], writeThreadKey)
        if lockExists == 0 then
            return 0;
        else
            local counter = redis.call('hincrby', KEYS[1], writeThreadKey, -1);
            if counter > 0 then
                redis.call('pexpire', KEYS[1], ARGV[1]);
                return 0;
            else
                redis.call('hdel', KEYS[1], writeThreadKey);
                -- only remain mode
                if (redis.call('hlen', KEYS[1]) == 1) then
                    redis.call('del', KEYS[1]);
                else
                    -- has unlocked self read-lock
                    redis.call('hset', KEYS[1], 'mode', 'read');
                end;
                return 1;
            end;
        end;
    end;
    return 0;
end