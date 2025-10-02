-- Lookup Account Script (Single Operation)
-- Redis Lua script to lookup a single account by ID
-- KEYS: none
-- ARGV[1]: account ID (string)

local account_id = ARGV[1]
local key = "account:" .. account_id

if redis.call('EXISTS', key) == 0 then
    return cjson.encode({})
end

local account = redis.call('HGETALL', key)

-- Convert array to map
local account_map = {}
for i = 1, #account, 2 do
    account_map[account[i]] = account[i + 1]
end

return cjson.encode({
    id = account_map.id,
    debits_pending = account_map.debits_pending,
    debits_posted = account_map.debits_posted,
    credits_pending = account_map.credits_pending,
    credits_posted = account_map.credits_posted,
    user_data_128 = account_map.user_data_128,
    user_data_64 = account_map.user_data_64,
    user_data_32 = account_map.user_data_32,
    ledger = account_map.ledger,
    code = account_map.code,
    flags = account_map.flags,
    timestamp = account_map.timestamp
})
