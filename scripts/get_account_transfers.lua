-- Get Account Transfers Script
-- Redis Lua script to get all transfers for an account
-- KEYS: none
-- ARGV[1]: account_id
-- ARGV[2]: (optional) timestamp_min (default: -inf)
-- ARGV[3]: (optional) timestamp_max (default: +inf)
-- ARGV[4]: (optional) limit (default: all, -1 means all)

local account_id = ARGV[1]
local timestamp_min = ARGV[2] or '-inf'
local timestamp_max = ARGV[3] or '+inf'
local limit = tonumber(ARGV[4]) or -1

-- Get transfer IDs from sorted set, ordered by timestamp
local transfer_ids
if limit == -1 then
    transfer_ids = redis.call('ZRANGEBYSCORE',
        'account:' .. account_id .. ':transfers',
        timestamp_min,
        timestamp_max)
else
    transfer_ids = redis.call('ZRANGEBYSCORE',
        'account:' .. account_id .. ':transfers',
        timestamp_min,
        timestamp_max,
        'LIMIT', 0, limit)
end

local results = {}

-- Lookup each transfer
for i, transfer_id in ipairs(transfer_ids) do
    local key = "transfer:" .. transfer_id

    if redis.call('EXISTS', key) == 1 then
        local transfer = redis.call('HGETALL', key)

        -- Convert array to map
        local transfer_map = {}
        for j = 1, #transfer, 2 do
            transfer_map[transfer[j]] = transfer[j + 1]
        end

        table.insert(results, {
            id = transfer_map.id,
            debit_account_id = transfer_map.debit_account_id,
            credit_account_id = transfer_map.credit_account_id,
            amount = transfer_map.amount,
            pending_id = transfer_map.pending_id,
            user_data_128 = transfer_map.user_data_128,
            user_data_64 = transfer_map.user_data_64,
            user_data_32 = transfer_map.user_data_32,
            timeout = transfer_map.timeout,
            ledger = transfer_map.ledger,
            code = transfer_map.code,
            flags = transfer_map.flags,
            timestamp = transfer_map.timestamp
        })
    end
end

return cjson.encode(results)
