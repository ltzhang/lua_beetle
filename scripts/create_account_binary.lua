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
    return string.char(32) .. string.rep('\0', 127) -- ERR_INVALID_DATA_SIZE = 32
end

-- Extract id (first 16 bytes) for key
local id = string.sub(account_data, 1, 16)
local key = "account:" .. id

-- Check if account already exists
if redis.call('EXISTS', key) == 1 then
    return string.char(21) .. string.rep('\0', 127) -- ERR_EXISTS = 21
end

-- Extract flags (2 bytes at offset 118)
local flags_byte1 = string.byte(account_data, 119)
local flags_byte2 = string.byte(account_data, 120)
local flags = flags_byte1 + flags_byte2 * 256

-- Check for LINKED flag (0x0001)
if (flags % 2) == 1 then
    return string.char(1) .. string.rep('\0', 127) -- ERR_LINKED_EVENT_CHAIN_OPEN = 1
end

-- Get current timestamp
local timestamp = redis.call('TIME')
local ts = tonumber(timestamp[1]) * 1000000000 + tonumber(timestamp[2]) * 1000

-- Build account data with timestamp (replace last 8 bytes)
local account_with_ts = string.sub(account_data, 1, 120) ..
                        string.char(
                            ts % 256,
                            math.floor(ts / 256) % 256,
                            math.floor(ts / 65536) % 256,
                            math.floor(ts / 16777216) % 256,
                            math.floor(ts / 4294967296) % 256,
                            math.floor(ts / 1099511627776) % 256,
                            math.floor(ts / 281474976710656) % 256,
                            math.floor(ts / 72057594037927936) % 256
                        )

-- Store as single binary string
redis.call('SET', key, account_with_ts)

-- Return success (ERR_OK = 0)
return string.char(0) .. string.rep('\0', 127)
