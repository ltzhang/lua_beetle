-- Lookup Transfer Script (Single Operation)
-- Redis Lua script to lookup a single transfer by ID
-- KEYS: none
-- ARGV[1]: transfer ID (string)

local transfer_id = ARGV[1]
local key = "transfer:" .. transfer_id

if redis.call('EXISTS', key) == 0 then
    return cjson.encode({})
end

local transfer = redis.call('HGETALL', key)

-- Convert array to map
local transfer_map = {}
for i = 1, #transfer, 2 do
    transfer_map[transfer[i]] = transfer[i + 1]
end

return cjson.encode({
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
