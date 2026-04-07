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
--   [44:46]   code (uint16, filter - 0 = match all)
--   [46:104]  reserved (58 bytes, must be 0)
--   [104:112] timestamp_min (uint64, inclusive)
--   [112:120] timestamp_max (uint64, inclusive)
--   [120:124] limit (uint32)
--   [124:128] flags (uint32): debits=0x01, credits=0x02, reversed=0x04)

local filter_data = ARGV[1]

-- Validate filter size
if #filter_data ~= 128 then
    return "" -- Invalid filter
end

-- Parse AccountFilter
local account_id = string.sub(filter_data, 1, 16)
local user_data_128_filter = string.sub(filter_data, 17, 32)
local user_data_64 = lb_decode_u64(filter_data, 33)
local user_data_32 = lb_decode_u32(filter_data, 41)
local code_filter = lb_decode_u16(filter_data, 45)
local timestamp_min = lb_decode_u64(filter_data, 105)
local timestamp_max = lb_decode_u64(filter_data, 113)
local limit = lb_decode_u32(filter_data, 121)
local flags = lb_decode_u32(filter_data, 125)

-- Parse flags
local FLAG_DEBITS = 0x01
local FLAG_CREDITS = 0x02
local FLAG_REVERSED = 0x04

local include_debits = lb_has_flag(flags, FLAG_DEBITS)
local include_credits = lb_has_flag(flags, FLAG_CREDITS)
local reversed = lb_has_flag(flags, FLAG_REVERSED)

local filter_user_data_128_active = user_data_128_filter ~= lb_zero_16

-- TigerBeetle treats invalid filters as empty results.
if account_id == lb_zero_16 or account_id == string.rep('\255', 16) then
    return ""
end

if limit == 0 then
    return "" -- Invalid limit
end

if not include_debits and not include_credits then
    return ""
end

if not lb_all_zero(filter_data, 47, 58) then
    return ""
end

if timestamp_max ~= 0 and timestamp_min > timestamp_max then
    return ""
end

-- Set timestamp_max to max value if not specified
if timestamp_max == 0 then
    timestamp_max = 2^63 - 1
end

-- Get transfer index from sorted set.
-- Member is lowercase 32-char hex transfer id, score is timestamp in microseconds.
local index_key = "account:" .. account_id .. ":transfers"
local range_result
local min_score = timestamp_min == 0 and "-inf" or tostring(math.floor(timestamp_min / 1000))
local max_score = timestamp_max == 0 and "+inf" or tostring(math.floor(timestamp_max / 1000))
if reversed then
    range_result = redis.call('ZREVRANGEBYSCORE', index_key, max_score, min_score, 'LIMIT', 0, limit)
else
    range_result = redis.call('ZRANGEBYSCORE', index_key, min_score, max_score, 'LIMIT', 0, limit)
end

if not range_result or #range_result == 0 then
    return "" -- No transfers found
end

local candidates = {}
local batch_keys = {}
local BATCH_SIZE = 64

for i = 1, #range_result do
    batch_keys[#batch_keys + 1] = "transfer:" .. range_result[i]

    if #batch_keys == BATCH_SIZE or i == #range_result then
        local fetched = redis.call('MGET', unpack(batch_keys))
        for j = 1, #fetched do
            local transfer = fetched[j]
            if transfer and #transfer == 128 then
                local debit_account_id = lb_slice16(transfer, 17)
                local credit_account_id = lb_slice16(transfer, 33)
                local timestamp = lb_decode_u64(transfer, 121)

                local include = false
                if include_debits and debit_account_id == account_id then
                    include = true
                end
                if include_credits and credit_account_id == account_id then
                    include = true
                end

                if include and timestamp >= timestamp_min and timestamp <= timestamp_max then
                    local matches = true

                    if filter_user_data_128_active and lb_slice16(transfer, 81) ~= user_data_128_filter then
                        matches = false
                    end

                    if matches and user_data_64 ~= 0 and lb_decode_u64(transfer, 97) ~= user_data_64 then
                        matches = false
                    end

                    if matches and user_data_32 ~= 0 and lb_decode_u32(transfer, 105) ~= user_data_32 then
                        matches = false
                    end

                        if matches and code_filter ~= 0 and lb_decode_u16(transfer, 117) ~= code_filter then
                            matches = false
                        end

                    if matches then
                        candidates[#candidates + 1] = {timestamp = timestamp, data = transfer}
                    end
                end
            end
        end

        batch_keys = {}
    end
end

if #candidates == 0 then
    return "" -- No matching transfers
end

local results = {}
for i = 1, math.min(limit, #candidates) do
    results[#results + 1] = candidates[i].data
end

-- Return concatenated binary transfers
return table.concat(results)
