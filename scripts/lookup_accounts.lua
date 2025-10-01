-- Lookup Accounts Script
-- Redis Lua script to lookup accounts by IDs
-- KEYS: none
-- ARGV: JSON array of account IDs

local account_ids = cjson.decode(ARGV[1])
local results = {}

for i, account_id in ipairs(account_ids) do
    local key = "account:" .. account_id

    if redis.call('EXISTS', key) == 1 then
        local account = redis.call('HGETALL', key)

        -- Convert array to map
        local account_map = {}
        for j = 1, #account, 2 do
            account_map[account[j]] = account[j + 1]
        end

        table.insert(results, {
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
    end
end

return cjson.encode(results)
