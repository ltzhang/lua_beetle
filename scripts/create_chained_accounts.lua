-- Create Chained Accounts Script (Binary Encoding)
-- Handles arrays of accounts with LINKED flag support for atomic chains
-- KEYS: none
-- ARGV[1]: concatenated binary account data (128 bytes per account)

-- Binary layout (128 bytes per account):
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

local accounts_data = ARGV[1]
local data_len = #accounts_data

-- Validate size is multiple of 128
if data_len % 128 ~= 0 then
    return string.char(32) .. string.rep('\0', 127) -- ERR_INVALID_DATA_SIZE = 32
end

local num_accounts = data_len / 128
local results = {}
local chain_start = nil
local created_accounts = {}

-- Get timestamp once for all accounts (will be used for non-imported accounts)
-- TODO: EloqKV doesn't support TIME command in Lua scripts, using arbitrary timestamp
-- local timestamp = redis.call('TIME')
-- local ts = tonumber(timestamp[1]) * 1000000000 + tonumber(timestamp[2]) * 1000
local ts = 1000000000000000000  -- Arbitrary timestamp for EloqKV compatibility
local ts_bytes = string.char(
    ts % 256,
    math.floor(ts / 256) % 256,
    math.floor(ts / 65536) % 256,
    math.floor(ts / 16777216) % 256,
    math.floor(ts / 4294967296) % 256,
    math.floor(ts / 1099511627776) % 256,
    math.floor(ts / 281474976710656) % 256,
    math.floor(ts / 72057594037927936) % 256
)

local IMPORTED_FLAG = 0x0002

for i = 0, num_accounts - 1 do
    local offset = i * 128 + 1
    local account_data = string.sub(accounts_data, offset, offset + 127)

    -- Extract id and flags
    local id = string.sub(account_data, 1, 16)
    local flags = string.byte(account_data, 119) + string.byte(account_data, 120) * 256
    local is_linked = (flags % 2) == 1  -- LINKED flag is 0x0001
    local is_imported = (flags % 4) >= 2  -- IMPORTED flag is 0x0002

    local error_code = 0
    local key = "account:" .. id

    -- Start new chain if this is a linked event
    if chain_start == nil and is_linked then
        chain_start = i
    end

    -- Check if account exists
    if redis.call('EXISTS', key) == 1 then
        error_code = 21 -- ERR_EXISTS
    end

    -- If error occurred, handle rollback for linked chains
    if error_code ~= 0 then
        if chain_start ~= nil then
            -- Rollback all accounts in the chain
            for j = chain_start, i - 1 do
                local rollback_offset = j * 128 + 1
                local rollback_id = string.sub(accounts_data, rollback_offset, rollback_offset + 15)
                redis.call('DEL', "account:" .. rollback_id)
            end

            -- Mark all accounts in chain as failed
            for j = chain_start, i do
                if j == i then
                    table.insert(results, string.char(error_code) .. string.rep('\0', 127))
                else
                    table.insert(results, string.char(1) .. string.rep('\0', 127)) -- ERR_LINKED_EVENT_FAILED
                end
            end

            chain_start = nil
        else
            -- Single account failure
            table.insert(results, string.char(error_code) .. string.rep('\0', 127))
        end
    else
        -- Success - create account with timestamp
        local account_with_ts
        if is_imported then
            -- Use client-provided timestamp (must be non-zero)
            account_with_ts = account_data
        else
            -- Server sets timestamp
            account_with_ts = string.sub(account_data, 1, 120) .. ts_bytes
        end

        redis.call('SET', key, account_with_ts)
        table.insert(created_accounts, id)
        table.insert(results, string.char(0) .. string.rep('\0', 127)) -- ERR_OK

        -- End chain if this account is not linked
        if not is_linked and chain_start ~= nil then
            chain_start = nil
        end
    end
end

-- Check for unclosed chain at end
if chain_start ~= nil then
    -- Rollback entire unclosed chain
    for j = chain_start, num_accounts - 1 do
        local rollback_offset = j * 128 + 1
        local rollback_id = string.sub(accounts_data, rollback_offset, rollback_offset + 15)
        redis.call('DEL', "account:" .. rollback_id)
    end

    -- Mark all as failed
    for j = chain_start, num_accounts - 1 do
        results[j + 1] = string.char(2) .. string.rep('\0', 127) -- ERR_LINKED_EVENT_CHAIN_OPEN
    end
end

-- Return concatenated results (128 bytes per result)
return table.concat(results)
