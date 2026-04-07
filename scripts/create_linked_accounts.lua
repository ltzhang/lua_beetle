-- Create Linked Accounts Script (Binary Encoding)
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

local ERR_INVALID_DATA_SIZE = 32
local ERR_LINKED_EVENT_FAILED = 1
local ERR_LINKED_EVENT_CHAIN_OPEN = 2
local ERR_IMPORTED_EVENT_TIMESTAMP_OUT_OF_RANGE = 24
local ERR_TIMESTAMP_MUST_BE_ZERO = 3
local ERR_RESERVED_FIELD = 4
local ERR_RESERVED_FLAG = 5
local ERR_ID_MUST_NOT_BE_ZERO = 6
local ERR_ID_MUST_NOT_BE_INT_MAX = 7
local ERR_FLAGS_ARE_MUTUALLY_EXCLUSIVE = 8
local ERR_DEBITS_PENDING_MUST_BE_ZERO = 9
local ERR_DEBITS_POSTED_MUST_BE_ZERO = 10
local ERR_CREDITS_PENDING_MUST_BE_ZERO = 11
local ERR_CREDITS_POSTED_MUST_BE_ZERO = 12
local ERR_LEDGER_MUST_NOT_BE_ZERO = 13
local ERR_CODE_MUST_NOT_BE_ZERO = 14
local ERR_EXISTS_WITH_DIFFERENT_FLAGS = 15
local ERR_EXISTS_WITH_DIFFERENT_USER_DATA_128 = 16
local ERR_EXISTS_WITH_DIFFERENT_USER_DATA_64 = 17
local ERR_EXISTS_WITH_DIFFERENT_USER_DATA_32 = 18
local ERR_EXISTS_WITH_DIFFERENT_LEDGER = 19
local ERR_EXISTS_WITH_DIFFERENT_CODE = 20
local ERR_EXISTS = 21

-- Validate size is multiple of 128
if data_len % 128 ~= 0 then
    return lb_result(ERR_INVALID_DATA_SIZE)
end

local num_accounts = data_len / 128
local results = {}
local chain_start = nil

-- Get timestamp once for all accounts (will be used for non-imported accounts)
local timestamp = redis.call('TIME')
local ts_bytes = lb_encode_u64(tonumber(timestamp[1]) * 1000000000 + tonumber(timestamp[2]) * 1000)

local IMPORTED_FLAG = 0x0010
local FLAG_LINKED = 0x0001
local FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS = 0x0002
local FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS = 0x0004
local ACCOUNT_FLAGS_MASK = 0x003f

for i = 0, num_accounts - 1 do
    local offset = i * 128 + 1
    local account_data = string.sub(accounts_data, offset, offset + 127)

    -- Extract id and flags
    local id = string.sub(account_data, 1, 16)
    local flags = string.byte(account_data, 119) + string.byte(account_data, 120) * 256
    local is_linked = lb_has_flag(flags, FLAG_LINKED)
    local is_imported = lb_has_flag(flags, IMPORTED_FLAG)

    local error_code = 0
    local key = "account:" .. id

    -- Start new chain if this is a linked event
    if chain_start == nil and is_linked then
        chain_start = i
    end

    if flags > ACCOUNT_FLAGS_MASK then
        error_code = ERR_RESERVED_FLAG
    elseif id == lb_zero_16 then
        error_code = ERR_ID_MUST_NOT_BE_ZERO
    elseif id == string.rep('\255', 16) then
        error_code = ERR_ID_MUST_NOT_BE_INT_MAX
    elseif not lb_all_zero(account_data, 17, 16) then
        error_code = ERR_DEBITS_PENDING_MUST_BE_ZERO
    elseif not lb_all_zero(account_data, 33, 16) then
        error_code = ERR_DEBITS_POSTED_MUST_BE_ZERO
    elseif not lb_all_zero(account_data, 49, 16) then
        error_code = ERR_CREDITS_PENDING_MUST_BE_ZERO
    elseif not lb_all_zero(account_data, 65, 16) then
        error_code = ERR_CREDITS_POSTED_MUST_BE_ZERO
    elseif not lb_all_zero(account_data, 109, 4) then
        error_code = ERR_RESERVED_FIELD
    elseif lb_decode_u32(account_data, 113) == 0 then
        error_code = ERR_LEDGER_MUST_NOT_BE_ZERO
    elseif lb_decode_u16(account_data, 117) == 0 then
        error_code = ERR_CODE_MUST_NOT_BE_ZERO
    elseif lb_has_flag(flags, FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS) and
           lb_has_flag(flags, FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS) then
        error_code = ERR_FLAGS_ARE_MUTUALLY_EXCLUSIVE
    else
        local client_timestamp = lb_decode_u64(account_data, 121)
        if is_imported then
            -- Imported-event ordering semantics are not implemented in lua_beetle.
            error_code = ERR_RESERVED_FLAG
        elseif client_timestamp ~= 0 then
            error_code = ERR_TIMESTAMP_MUST_BE_ZERO
        end
    end

    local existing = nil
    if error_code == 0 then
        existing = redis.call('GET', key)
        if existing and #existing == 128 then
            local existing_flags = lb_decode_u16(existing, 119)
            if existing_flags ~= flags then
                error_code = ERR_EXISTS_WITH_DIFFERENT_FLAGS
            elseif string.sub(existing, 81, 96) ~= string.sub(account_data, 81, 96) then
                error_code = ERR_EXISTS_WITH_DIFFERENT_USER_DATA_128
            elseif lb_decode_u64(existing, 97) ~= lb_decode_u64(account_data, 97) then
                error_code = ERR_EXISTS_WITH_DIFFERENT_USER_DATA_64
            elseif lb_decode_u32(existing, 105) ~= lb_decode_u32(account_data, 105) then
                error_code = ERR_EXISTS_WITH_DIFFERENT_USER_DATA_32
            elseif lb_decode_u32(existing, 113) ~= lb_decode_u32(account_data, 113) then
                error_code = ERR_EXISTS_WITH_DIFFERENT_LEDGER
            elseif lb_decode_u16(existing, 117) ~= lb_decode_u16(account_data, 117) then
                error_code = ERR_EXISTS_WITH_DIFFERENT_CODE
            else
                error_code = ERR_EXISTS
            end
        end
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
                    results[#results + 1] = lb_result(error_code)
                else
                    results[#results + 1] = lb_result(ERR_LINKED_EVENT_FAILED)
                end
            end

            chain_start = nil
        else
            -- Single account failure
            results[#results + 1] = lb_result(error_code)
        end
    else
        -- Success - create account with timestamp
        local account_with_ts
        if is_imported then
            account_with_ts = account_data
        else
            -- Server sets timestamp
            account_with_ts = string.sub(account_data, 1, 120) .. ts_bytes
        end

        redis.call('SET', key, account_with_ts)
        results[#results + 1] = lb_result(0) -- ERR_OK

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
        if j == num_accounts - 1 then
            results[j + 1] = lb_result(ERR_LINKED_EVENT_CHAIN_OPEN)
        else
            results[j + 1] = lb_result(ERR_LINKED_EVENT_FAILED)
        end
    end
end

-- Return concatenated results (128 bytes per result)
return table.concat(results)
