-- Get Account Balances Script (Binary Encoding)
-- Matches TigerBeetle's get_account_balances with AccountFilter
-- Returns concatenated 64-byte binary AccountBalance data
-- KEYS: none
-- ARGV[1]: AccountFilter (128 bytes binary)
--
-- AccountFilter layout (128 bytes):
--   [0:16]    account_id (uint128)
--   [16:32]   user_data_128 (uint128, filter - 0 = match all)
--   [32:40]   user_data_64 (uint64, filter - 0 = match all)
--   [40:44]   user_data_32 (uint32, filter - 0 = match all)
--   [44:46]   reserved (uint16, must be 0)
--   [46:48]   code (uint16, filter - 0 = match all)
--   [48:56]   timestamp_min (uint64, inclusive)
--   [56:64]   timestamp_max (uint64, inclusive)
--   [64:68]   limit (uint32)
--   [68:72]   flags (uint32): debits=0x01, credits=0x02, reversed=0x04
--   [72:128]  reserved (56 bytes, must be 0)
--
-- AccountBalance layout (64 bytes):
--   [0:8]     timestamp (uint64)
--   [8:24]    debits_pending (uint128)
--   [24:40]   debits_posted (uint128)
--   [40:56]   credits_pending (uint128)
--   [56:64]   credits_posted (uint128)

local filter_data = ARGV[1]

-- Validate filter size
if #filter_data ~= 128 then
    return "" -- Invalid filter
end

-- Helper: decode uint64 from bytes (little-endian)
local function decode_u64(data, offset)
    local value = 0
    for i = 0, 7 do
        value = value + string.byte(data, offset + i) * (2 ^ (i * 8))
    end
    return value
end

-- Helper: decode uint32 from bytes (little-endian)
local function decode_u32(data, offset)
    local value = 0
    for i = 0, 3 do
        value = value + string.byte(data, offset + i) * (2 ^ (i * 8))
    end
    return value
end

-- Helper: convert 16-byte binary ID to hex string for Redis keys
local function id_to_string(id_bytes)
    local hex = ""
    for i = 1, #id_bytes do
        hex = hex .. string.format("%02x", string.byte(id_bytes, i))
    end
    return hex
end

-- Parse AccountFilter
local account_id = string.sub(filter_data, 1, 16)
local timestamp_min = decode_u64(filter_data, 49)
local timestamp_max = decode_u64(filter_data, 57)
local limit = decode_u32(filter_data, 65)
local flags = decode_u32(filter_data, 69)

-- Parse flags
local FLAG_REVERSED = 0x04
local reversed = (math.floor(flags / FLAG_REVERSED) % 2) == 1

-- Validate limit
if limit == 0 then
    return "" -- Invalid limit
end

-- Set timestamp_max to max value if not specified
if timestamp_max == 0 or timestamp_max >= (2^63) then
    timestamp_max = 2^63 - 1
end

-- Check if account has history flag enabled
-- Use binary account_id for key
local account_key = "account:" .. account_id
local account_data = redis.call('GET', account_key)

if not account_data or #account_data ~= 128 then
    return "" -- Account not found
end

-- Extract account flags (bytes 119-120, 1-indexed: 119-120)
local account_flags_byte1 = string.byte(account_data, 119)
local account_flags_byte2 = string.byte(account_data, 120)
local account_flags = account_flags_byte1 + account_flags_byte2 * 256

-- Account flag for history: 0x08
local ACCOUNT_FLAG_HISTORY = 0x08
local has_history = (math.floor(account_flags / ACCOUNT_FLAG_HISTORY) % 2) == 1

if not has_history then
    return "" -- Account doesn't have history flag set
end

-- Get balance history index (string of concatenated 64-byte balance snapshots)
-- Use binary account_id for key (same as create_transfer.lua)
local index_key = "account:" .. account_id .. ":balance_history"
local balance_blob = redis.call('GET', index_key)

if not balance_blob or #balance_blob == 0 then
    return "" -- No balance history found
end

-- Each balance snapshot is 64 bytes
local num_balances = #balance_blob / 64

-- Filter and collect balances
local candidates = {}
for i = 0, num_balances - 1 do
    local balance_data = string.sub(balance_blob, i * 64 + 1, i * 64 + 64)
    local timestamp = decode_u64(balance_data, 1)

    -- Apply timestamp filter
    if timestamp >= timestamp_min and timestamp <= timestamp_max then
        table.insert(candidates, {timestamp = timestamp, data = balance_data})
    end
end

if #candidates == 0 then
    return "" -- No matching balances
end

-- Sort by timestamp (at query time)
table.sort(candidates, function(a, b)
    if reversed then
        return a.timestamp > b.timestamp
    else
        return a.timestamp < b.timestamp
    end
end)

-- Apply limit and extract balance data
local results = {}
for i = 1, math.min(limit, #candidates) do
    table.insert(results, candidates[i].data)
end

-- Return concatenated binary balances
return table.concat(results)
