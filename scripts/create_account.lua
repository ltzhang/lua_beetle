-- Create Account Script (Binary Encoding)
-- Binary layout matches TigerBeetle format: 128 bytes fixed size
-- KEYS: none
-- ARGV[1]: account binary data (128 bytes)

-- Binary layout (128 bytes total):
-- id: 16 bytes (offset 0)
-- debits_pending: 16 bytes (offset 16)
-- debits_posted: 16 bytes (offset 32)
-- credits_pending: 16 bytes (offset 48)
-- credits_posted: 16 bytes (offset 64)
-- user_data_128: 16 bytes (offset 80)
-- user_data_64: 8 bytes (offset 96)
-- user_data_32: 4 bytes (offset 104)
-- reserved: 4 bytes (offset 108)
-- ledger: 4 bytes (offset 112)
-- code: 2 bytes (offset 116)
-- flags: 2 bytes (offset 118)
-- timestamp: 8 bytes (offset 120)

local account_data = ARGV[1]

-- Validate size
if #account_data ~= 128 then
    return lb_result(32) -- ERR_INVALID_DATA_SIZE
end

-- Extract id (first 16 bytes) for key
local id = string.sub(account_data, 1, 16)
local key = "account:" .. id

-- Check if account already exists
if redis.call('EXISTS', key) == 1 then
    return lb_result(21) -- ERR_EXISTS
end

-- Extract flags (2 bytes at offset 118)
local flags_byte1 = string.byte(account_data, 119)
local flags_byte2 = string.byte(account_data, 120)
local flags = flags_byte1 + flags_byte2 * 256

-- Check for LINKED flag (0x0001)
if lb_has_flag(flags, 0x0001) then
    return lb_result(1) -- ERR_LINKED_EVENT_CHAIN_OPEN
end

-- Prepare account data with timestamp
local account_with_ts
local IMPORTED_FLAG = 0x0100
local DEFAULT_TIMESTAMP = lb_encode_u64(1000000000000000000)

-- Only set timestamp if imported flag is NOT set
if not lb_has_flag(flags, IMPORTED_FLAG) then
    -- imported flag is NOT set, server sets timestamp
    -- TODO: EloqKV doesn't support TIME command in Lua scripts, using arbitrary timestamp
    account_with_ts = string.sub(account_data, 1, 120) .. DEFAULT_TIMESTAMP
else
    -- imported flag IS set, use client-provided timestamp (must be non-zero)
    local ts = lb_decode_u64(account_data, 121)

    -- Timestamp must be non-zero when imported flag is set
    if ts == 0 then
        return lb_result(32) -- ERR_INVALID_DATA_SIZE
    end

    account_with_ts = account_data
end

-- Store as single binary string
redis.call('SET', key, account_with_ts)

-- Return success (ERR_OK = 0)
return lb_result(0)
