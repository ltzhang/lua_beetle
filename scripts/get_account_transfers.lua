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

-- Parse AccountFilter
local account_id = string.sub(filter_data, 1, 16)
local user_data_128_filter = string.sub(filter_data, 17, 32)
local user_data_64 = lb_decode_u64(filter_data, 33)
local user_data_32 = lb_decode_u32(filter_data, 41)
local code_filter = lb_decode_u16(filter_data, 47)
local timestamp_min = lb_decode_u64(filter_data, 49)
local timestamp_max = lb_decode_u64(filter_data, 57)
local limit = lb_decode_u32(filter_data, 65)
local flags = lb_decode_u32(filter_data, 69)

-- Parse flags
local FLAG_DEBITS = 0x01
local FLAG_CREDITS = 0x02
local FLAG_REVERSED = 0x04

local include_debits = lb_has_flag(flags, FLAG_DEBITS)
local include_credits = lb_has_flag(flags, FLAG_CREDITS)
local reversed = lb_has_flag(flags, FLAG_REVERSED)

-- If neither debits nor credits specified, include both
if not include_debits and not include_credits then
    include_debits = true
    include_credits = true
end

local filter_user_data_128_active = user_data_128_filter ~= lb_zero_16

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
local transfer_ids = {}
for i = 1, num_transfers do
    local offset = (i - 1) * 16 + 1
    transfer_ids[i] = string.sub(transfer_ids_blob, offset, offset + 15)
end

local candidates = {}
local batch_keys = {}
local batch_ids = {}
local BATCH_SIZE = 64

for i = 1, num_transfers do
    local raw_id = transfer_ids[i]
    batch_ids[#batch_ids + 1] = raw_id
    batch_keys[#batch_keys + 1] = "transfer:" .. lb_hex16(raw_id)

    if #batch_keys == BATCH_SIZE or i == num_transfers then
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

        batch_ids = {}
        batch_keys = {}
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
    results[#results + 1] = candidates[i].data
end

-- Return concatenated binary transfers
return table.concat(results)
