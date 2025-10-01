-- Get Account Balances Script
-- Redis Lua script to get balance history for an account
-- Returns current balance if no HISTORY flag, or full history if HISTORY flag is set
-- KEYS: none
-- ARGV[1]: account_id
-- ARGV[2]: (optional) timestamp_min (default: -inf)
-- ARGV[3]: (optional) timestamp_max (default: +inf)
-- ARGV[4]: (optional) limit (default: all, -1 means all)

local account_id = ARGV[1]
local timestamp_min = ARGV[2] or '-inf'
local timestamp_max = ARGV[3] or '+inf'
local limit = tonumber(ARGV[4]) or -1

-- Check if account exists
local account_key = 'account:' .. account_id
if redis.call('EXISTS', account_key) == 0 then
    return cjson.encode({
        error = "ACCOUNT_NOT_FOUND"
    })
end

-- Get account data
local account = redis.call('HGETALL', account_key)
local account_map = {}
for i = 1, #account, 2 do
    account_map[account[i]] = account[i + 1]
end

local account_flags = tonumber(account_map.flags) or 0
local ACCOUNT_FLAG_HISTORY = 8
local has_history = (account_flags % (ACCOUNT_FLAG_HISTORY * 2)) >= ACCOUNT_FLAG_HISTORY

-- If no HISTORY flag, return current balance only
if not has_history then
    return cjson.encode({
        account_id = account_id,
        current_balance = {
            debits_pending = account_map.debits_pending,
            debits_posted = account_map.debits_posted,
            credits_pending = account_map.credits_pending,
            credits_posted = account_map.credits_posted,
            timestamp = account_map.timestamp
        },
        history = {}
    })
end

-- If HISTORY flag is set, return balance history
local balance_entries
if limit == -1 then
    balance_entries = redis.call('ZRANGEBYSCORE',
        'account:' .. account_id .. ':balance_history',
        timestamp_min,
        timestamp_max)
else
    balance_entries = redis.call('ZRANGEBYSCORE',
        'account:' .. account_id .. ':balance_history',
        timestamp_min,
        timestamp_max,
        'LIMIT', 0, limit)
end

local history = {}
for i, entry_json in ipairs(balance_entries) do
    local entry = cjson.decode(entry_json)
    table.insert(history, entry)
end

return cjson.encode({
    account_id = account_id,
    current_balance = {
        debits_pending = account_map.debits_pending,
        debits_posted = account_map.debits_posted,
        credits_pending = account_map.credits_pending,
        credits_posted = account_map.credits_posted,
        timestamp = account_map.timestamp
    },
    history = history
})
