-- Get Account Balances Script (Binary Encoding)
-- Returns current balance for an account
-- KEYS: none
-- ARGV[1]: account_id (16 bytes binary)

local account_id = ARGV[1]

-- Check if account exists
local account_key = 'account:' .. account_id
local account = redis.call('GET', account_key)

if not account or #account ~= 128 then
    -- Return empty result to indicate account not found
    return ""
end

-- Return the raw 128-byte account data
-- The caller can extract the balance fields:
-- debits_pending: bytes 16-31
-- debits_posted: bytes 32-47
-- credits_pending: bytes 48-63
-- credits_posted: bytes 64-79
-- timestamp: bytes 120-127
return account
