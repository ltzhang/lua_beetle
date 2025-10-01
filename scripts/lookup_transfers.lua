-- Lookup Transfers Script
-- Redis Lua script to lookup transfers by IDs
-- KEYS: none
-- ARGV: JSON array of transfer IDs

local transfer_ids = cjson.decode(ARGV[1])
local results = {}

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
