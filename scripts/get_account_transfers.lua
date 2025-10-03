-- Get Account Transfers Script (Binary Encoding)
-- Matches TigerBeetle's get_account_transfers with AccountFilter
-- Returns concatenated 128-byte binary transfer data
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

local filter_data = ARGV[1]

-- Validate filter size
if #filter_data ~= 128 then
    return "" -- Invalid filter
end

-- Helper: decode uint128 from bytes (little-endian)
local function decode_u128(data, offset)
    local low = 0
    local high = 0
    for i = 0, 7 do
        low = low + string.byte(data, offset + i) * (2 ^ (i * 8))
    end
    for i = 0, 7 do
        high = high + string.byte(data, offset + 8 + i) * (2 ^ (i * 8))
    end
    return {low = low, high = high}
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

-- Helper: decode uint16 from bytes (little-endian)
local function decode_u16(data, offset)
    return string.byte(data, offset) + string.byte(data, offset + 1) * 256
end

-- Helper: compare u128 values (returns -1, 0, or 1)
local function compare_u128(a, b)
    if a.high < b.high then return -1 end
    if a.high > b.high then return 1 end
    if a.low < b.low then return -1 end
    if a.low > b.low then return 1 end
    return 0
end

-- Helper: check if u128 is zero
local function is_u128_zero(v)
    return v.low == 0 and v.high == 0
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
local user_data_128 = decode_u128(filter_data, 17)
local user_data_64 = decode_u64(filter_data, 33)
local user_data_32 = decode_u32(filter_data, 41)
local code_filter = decode_u16(filter_data, 47)
local timestamp_min = decode_u64(filter_data, 49)
local timestamp_max = decode_u64(filter_data, 57)
local limit = decode_u32(filter_data, 65)
local flags = decode_u32(filter_data, 69)

-- Parse flags
local FLAG_DEBITS = 0x01
local FLAG_CREDITS = 0x02
local FLAG_REVERSED = 0x04

local include_debits = (flags % (FLAG_DEBITS * 2)) >= FLAG_DEBITS
local include_credits = (math.floor(flags / FLAG_CREDITS) % 2) == 1
local reversed = (math.floor(flags / FLAG_REVERSED) % 2) == 1

-- If neither debits nor credits specified, include both
if not include_debits and not include_credits then
    include_debits = true
    include_credits = true
end

-- Validate limit
if limit == 0 then
    return "" -- Invalid limit
end

-- Set timestamp_max to max value if not specified
if timestamp_max == 0 or timestamp_max >= (2^63) then
    timestamp_max = 2^63 - 1
end

-- Get transfer index (string of concatenated 16-byte transfer IDs)
-- Use binary account_id for key (same as create_transfer.lua)
local index_key = "account:" .. account_id .. ":transfers"
local transfer_ids_blob = redis.call('GET', index_key)

if not transfer_ids_blob or #transfer_ids_blob == 0 then
    return "" -- No transfers found
end

-- Each transfer ID is 16 bytes
local num_transfers = #transfer_ids_blob / 16

-- Fetch all transfers and filter
local candidates = {}
for i = 0, num_transfers - 1 do
    local transfer_id_raw = string.sub(transfer_ids_blob, i * 16 + 1, i * 16 + 16)
    local transfer_id_hex = id_to_string(transfer_id_raw)
    local transfer_key = "transfer:" .. transfer_id_hex
    local transfer = redis.call('GET', transfer_key)

    if transfer and #transfer == 128 then
        -- Extract transfer fields for filtering
        local debit_account_id = string.sub(transfer, 17, 32)
        local credit_account_id = string.sub(transfer, 33, 48)
        local timestamp = decode_u64(transfer, 121)

        -- Check if this transfer involves the account as debit or credit
        local is_debit = (debit_account_id == account_id)
        local is_credit = (credit_account_id == account_id)

        -- Apply debits/credits filter
        local include = false
        if is_debit and include_debits then
            include = true
        end
        if is_credit and include_credits then
            include = true
        end

        if include then
            -- Apply timestamp filter
            if timestamp >= timestamp_min and timestamp <= timestamp_max then
                -- Apply optional filters
                local matches = true

                -- Filter by user_data_128
                if not is_u128_zero(user_data_128) then
                    local transfer_user_data_128 = decode_u128(transfer, 81)
                    if compare_u128(transfer_user_data_128, user_data_128) ~= 0 then
                        matches = false
                    end
                end

                -- Filter by user_data_64
                if matches and user_data_64 ~= 0 then
                    local transfer_user_data_64 = decode_u64(transfer, 97)
                    if transfer_user_data_64 ~= user_data_64 then
                        matches = false
                    end
                end

                -- Filter by user_data_32
                if matches and user_data_32 ~= 0 then
                    local transfer_user_data_32 = decode_u32(transfer, 105)
                    if transfer_user_data_32 ~= user_data_32 then
                        matches = false
                    end
                end

                -- Filter by code
                if matches and code_filter ~= 0 then
                    local transfer_code = decode_u16(transfer, 117)
                    if transfer_code ~= code_filter then
                        matches = false
                    end
                end

                if matches then
                    table.insert(candidates, {timestamp = timestamp, data = transfer})
                end
            end
        end
    end
end

if #candidates == 0 then
    return "" -- No matching transfers
end

-- Sort by timestamp (at query time)
table.sort(candidates, function(a, b)
    if reversed then
        return a.timestamp > b.timestamp
    else
        return a.timestamp < b.timestamp
    end
end)

-- Apply limit and extract transfer data
local results = {}
for i = 1, math.min(limit, #candidates) do
    table.insert(results, candidates[i].data)
end

-- Return concatenated binary transfers
return table.concat(results)
