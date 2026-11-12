local existing = redis.call('GET', KEYS[1])
if existing then
    if existing == 'cancelled' then
        return {0, 'cancelled'}
    end
    return {0, 'replay', existing}
end

local velocity = redis.call('INCR', KEYS[2])
if velocity == 1 then
    redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[2]))
end
if velocity > tonumber(ARGV[3]) then
    redis.call('DECR', KEYS[2])
    return {-1, 'velocity_limit'}
end

redis.call('SET', KEYS[1], ARGV[1], 'PX', tonumber(ARGV[2]))
return {1, 'claimed', tostring(velocity)}
