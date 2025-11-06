-- Create Linked Transfers Script (Binary Encoding)
-- Handles arrays of transfers with LINKED flag support for atomic chains
-- KEYS: none
-- ARGV[1]: concatenated binary transfer data (128 bytes per transfer)

-- Transfer binary layout (128 bytes):
-- id: 16 bytes (offset 0)
-- debit_account_id: 16 bytes (offset 16)
-- credit_account_id: 16 bytes (offset 32)
-- amount: 16 bytes (offset 48)
-- pending_id: 16 bytes (offset 64)
-- user_data_128: 16 bytes (offset 80)
-- user_data_64: 8 bytes (offset 96)
-- user_data_32: 4 bytes (offset 104)
-- timeout: 4 bytes (offset 108)
-- ledger: 4 bytes (offset 112)
-- code: 2 bytes (offset 116)
-- flags: 2 bytes (offset 118)
-- timestamp: 8 bytes (offset 120)

local transfers_data = ARGV[1]
local data_len = #transfers_data

-- Validate size is multiple of 128
if data_len % 128 ~= 0 then
    return string.char(32) .. string.rep('\0', 127) -- ERR_INVALID_DATA_SIZE = 32
end

local num_transfers = data_len / 128

-- Helper functions
local function extract_16bytes(data, offset)
    return string.sub(data, offset, offset + 15)
end

local function id_to_hex(id_bytes)
    local hex_chars = {}
    for i = 1, #id_bytes do
        hex_chars[i] = string.format("%02x", string.byte(id_bytes, i))
    end
    return table.concat(hex_chars)
end

local function get_u128(data, offset)
    return string.sub(data, offset, offset + 15)
end

local function add_u128_bytes(left, right)
    local carry = 0
    local out = {}
    for i = 1, 16 do
        local sum = string.byte(left, i) + string.byte(right, i) + carry
        out[i] = string.char(sum % 256)
        carry = math.floor(sum / 256)
    end
    return table.concat(out), carry
end

local function sub_u128_bytes(left, right)
    local borrow = 0
    local out = {}
    for i = 1, 16 do
        local diff = string.byte(left, i) - string.byte(right, i) - borrow
        if diff < 0 then
            diff = diff + 256
            borrow = 1
        else
            borrow = 0
        end
        out[i] = string.char(diff)
    end
    if borrow ~= 0 then
        return nil
    end
    return table.concat(out)
end

local function add_field(data, offset, value_bytes)
    local current = get_u128(data, offset)
    local sum, carry = add_u128_bytes(current, value_bytes)
    if carry ~= 0 then
        return nil
    end
    return sum
end

local function sub_field(data, offset, value_bytes)
    local current = get_u128(data, offset)
    return sub_u128_bytes(current, value_bytes)
end

local function compare_u128(left, right)
    for i = 16, 1, -1 do
        local a = string.byte(left, i)
        local b = string.byte(right, i)
        if a ~= b then
            if a > b then
                return 1
            else
                return -1
            end
        end
    end
    return 0
end

local ZERO_ID = string.rep('\0', 16)

-- Get timestamp once (for non-imported transfers)
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

local results = {}
local chain_start = nil
local modified_accounts = {}
local index_original_lengths = {} -- Track original lengths for rollback

local FLAG_PENDING = 0x0002
local FLAG_POST_PENDING = 0x0004
local FLAG_VOID_PENDING = 0x0008
local FLAG_IMPORTED = 0x0100
local ACCOUNT_FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS = 0x0002
local ACCOUNT_FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS = 0x0004

for i = 0, num_transfers - 1 do
    local offset = i * 128 + 1
    local transfer_data = string.sub(transfers_data, offset, offset + 127)

    -- Extract fields
    local transfer_id = extract_16bytes(transfer_data, 1)
    local transfer_id_hex = id_to_hex(transfer_id)
    local debit_account_id = extract_16bytes(transfer_data, 17)
    local credit_account_id = extract_16bytes(transfer_data, 33)
    local amount_bytes = extract_16bytes(transfer_data, 49)
    local flags = string.byte(transfer_data, 119) + string.byte(transfer_data, 120) * 256
    local is_linked = (flags % 2) == 1
    local is_pending = (math.floor(flags / FLAG_PENDING) % 2) == 1
    local is_imported = (math.floor(flags / FLAG_IMPORTED) % 2) == 1

    local error_code = 0

    -- Start chain tracking
    if chain_start == nil and is_linked then
        chain_start = i
    end

    -- Validate accounts are different
    if debit_account_id == credit_account_id then
        error_code = 40 -- ERR_ACCOUNTS_MUST_BE_DIFFERENT
    end

    -- Check if transfer exists
    if error_code == 0 then
        local transfer_key = "transfer:" .. transfer_id_hex
        if redis.call('EXISTS', transfer_key) == 1 then
            error_code = 29 -- ERR_EXISTS_WITH_DIFFERENT_FLAGS
        end
    end

    -- Load accounts
    local debit_account, credit_account
    if error_code == 0 then
        local debit_key = "account:" .. debit_account_id
        local credit_key = "account:" .. credit_account_id

        debit_account = redis.call('GET', debit_key)
        credit_account = redis.call('GET', credit_key)

        if not debit_account or #debit_account ~= 128 then
            error_code = 38 -- ERR_DEBIT_ACCOUNT_NOT_FOUND
        elseif not credit_account or #credit_account ~= 128 then
            error_code = 39 -- ERR_CREDIT_ACCOUNT_NOT_FOUND
        end
    end

    -- Validate and process transfer
    local new_debit_account, new_credit_account
    if error_code == 0 then
        -- Check ledgers match
        local transfer_ledger = string.sub(transfer_data, 113, 116)
        local debit_ledger = string.sub(debit_account, 113, 116)
        local credit_ledger = string.sub(credit_account, 113, 116)

        if transfer_ledger ~= debit_ledger or transfer_ledger ~= credit_ledger then
            error_code = 52 -- ERR_LEDGER_MUST_MATCH
        else
            new_debit_account = debit_account
            new_credit_account = credit_account

            local is_post = (math.floor(flags / FLAG_POST_PENDING) % 2) == 1
            local is_void = (math.floor(flags / FLAG_VOID_PENDING) % 2) == 1
            local pending_id_raw = extract_16bytes(transfer_data, 65)
            local pending_id_hex = id_to_hex(pending_id_raw)

            if is_post or is_void then
                if pending_id_raw == ZERO_ID then
                    error_code = 33 -- ERR_PENDING_ID_REQUIRED
                else
                    local pending_transfer_key = "transfer:" .. pending_id_hex
                    local pending_transfer = redis.call('GET', pending_transfer_key)
                    if not pending_transfer or #pending_transfer ~= 128 then
                        error_code = 34 -- ERR_PENDING_TRANSFER_NOT_FOUND
                    else
                        local pending_flags = string.byte(pending_transfer, 119) + string.byte(pending_transfer, 120) * 256
                        local pending_is_pending = (math.floor(pending_flags / FLAG_PENDING) % 2) == 1
                        if not pending_is_pending then
                            error_code = 35 -- ERR_PENDING_TRANSFER_ALREADY_POSTED
                        else
                            local pending_debit_id = get_u128(pending_transfer, 17)
                            local pending_credit_id = get_u128(pending_transfer, 33)
                            local pending_amount_bytes = extract_16bytes(pending_transfer, 49)

                            if pending_debit_id ~= debit_account_id or pending_credit_id ~= credit_account_id or pending_amount_bytes ~= amount_bytes then
                                error_code = 34
                            else
                                if is_post then
                                    local debit_pending = sub_field(debit_account, 17, pending_amount_bytes)
                                    if not debit_pending then
                                        error_code = 35
                                    else
                                        local credit_pending = sub_field(credit_account, 49, pending_amount_bytes)
                                        if not credit_pending then
                                            error_code = 35
                                        else
                                            local debit_posted = add_field(debit_account, 33, pending_amount_bytes)
                                            if not debit_posted then
                                                error_code = 42
                                            else
                                                local credit_posted = add_field(credit_account, 65, pending_amount_bytes)
                                                if not credit_posted then
                                                    error_code = 43
                                                else
                                                    new_debit_account = string.sub(debit_account, 1, 16) .. debit_pending .. debit_posted .. string.sub(debit_account, 49, 128)
                                                    new_credit_account = string.sub(credit_account, 1, 48) .. credit_pending .. credit_posted .. string.sub(credit_account, 81, 128)
                                                end
                                            end
                                        end
                                    end
                                else
                                    local debit_pending = sub_field(debit_account, 17, pending_amount_bytes)
                                    if not debit_pending then
                                        error_code = 36 -- ERR_PENDING_TRANSFER_ALREADY_VOIDED
                                    else
                                        local credit_pending = sub_field(credit_account, 49, pending_amount_bytes)
                                        if not credit_pending then
                                            error_code = 36
                                        else
                                            new_debit_account = string.sub(debit_account, 1, 16) .. debit_pending .. string.sub(debit_account, 33, 128)
                                            new_credit_account = string.sub(credit_account, 1, 48) .. credit_pending .. string.sub(credit_account, 65, 128)
                                        end
                                    end
                                end
                            end
                        end
                    end
                end
            elseif is_pending then
                local debit_pending = add_field(debit_account, 17, amount_bytes)
                if not debit_pending then
                    error_code = 42
                else
                    local credit_pending = add_field(credit_account, 49, amount_bytes)
                    if not credit_pending then
                        error_code = 43
                    else
                        new_debit_account = string.sub(debit_account, 1, 16) .. debit_pending .. string.sub(debit_account, 33, 128)
                        new_credit_account = string.sub(credit_account, 1, 48) .. credit_pending .. string.sub(credit_account, 65, 128)
                    end
                end
            else
                local debit_posted = add_field(debit_account, 33, amount_bytes)
                if not debit_posted then
                    error_code = 42
                else
                    local credit_posted = add_field(credit_account, 65, amount_bytes)
                    if not credit_posted then
                        error_code = 43
                    else
                        new_debit_account = string.sub(debit_account, 1, 32) .. debit_posted .. string.sub(debit_account, 49, 128)
                        new_credit_account = string.sub(credit_account, 1, 64) .. credit_posted .. string.sub(credit_account, 81, 128)
                    end
                end
            end

            if error_code == 0 then
                local debit_flags = string.byte(new_debit_account, 119) + string.byte(new_debit_account, 120) * 256
                local credit_flags = string.byte(new_credit_account, 119) + string.byte(new_credit_account, 120) * 256

                if (math.floor(debit_flags / ACCOUNT_FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS) % 2) == 1 then
                    local debit_posted_bytes = get_u128(new_debit_account, 33)
                    local debit_pending_bytes = get_u128(new_debit_account, 17)
                    local debit_total, debit_overflow = add_u128_bytes(debit_posted_bytes, debit_pending_bytes)
                    if debit_overflow ~= 0 then
                        error_code = 42
                    else
                        local credit_posted_bytes = get_u128(new_debit_account, 65)
                        local credit_pending_bytes = get_u128(new_debit_account, 49)
                        local credit_total, credit_overflow = add_u128_bytes(credit_posted_bytes, credit_pending_bytes)
                        if credit_overflow ~= 0 then
                            error_code = 43
                        elseif compare_u128(debit_total, credit_total) == 1 then
                            error_code = 42
                        end
                    end
                end

                if error_code == 0 and (math.floor(credit_flags / ACCOUNT_FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS) % 2) == 1 then
                    local credit_posted_bytes = get_u128(new_credit_account, 65)
                    local credit_pending_bytes = get_u128(new_credit_account, 49)
                    local credit_total, credit_overflow = add_u128_bytes(credit_posted_bytes, credit_pending_bytes)
                    if credit_overflow ~= 0 then
                        error_code = 43
                    else
                        local debit_posted_bytes = get_u128(new_credit_account, 33)
                        local debit_pending_bytes = get_u128(new_credit_account, 17)
                        local debit_total, debit_overflow = add_u128_bytes(debit_posted_bytes, debit_pending_bytes)
                        if debit_overflow ~= 0 then
                            error_code = 42
                        elseif compare_u128(credit_total, debit_total) == 1 then
                            error_code = 43
                        end
                    end
                end
            end
        end
    end

    -- Handle result
    if error_code ~= 0 then
        if chain_start ~= nil then
            -- Rollback chain
            for j = chain_start, i - 1 do
                local rb_offset = j * 128 + 1
                local rb_id = extract_16bytes(transfers_data, rb_offset)
                local rb_id_hex = id_to_hex(rb_id)
                redis.call('DEL', "transfer:" .. rb_id_hex)

                -- Restore modified accounts
                local rb_debit_id = extract_16bytes(transfers_data, rb_offset + 16)
                local rb_credit_id = extract_16bytes(transfers_data, rb_offset + 32)

                if modified_accounts[rb_debit_id] then
                    redis.call('SET', "account:" .. rb_debit_id, modified_accounts[rb_debit_id])
                end
                if modified_accounts[rb_credit_id] then
                    redis.call('SET', "account:" .. rb_credit_id, modified_accounts[rb_credit_id])
                end
            end

            -- Rollback indexes by truncating to original lengths
            for key, original_len in pairs(index_original_lengths) do
                if original_len == 0 then
                    redis.call('DEL', key)
                else
                    -- Truncate by getting the prefix and setting it back
                    local truncated = redis.call('GETRANGE', key, 0, original_len - 1)
                    redis.call('SET', key, truncated)
                end
            end

            -- Mark all in chain as failed
            for j = chain_start, i do
                if j == i then
                    results[j + 1] = string.char(error_code) .. string.rep('\0', 127)
                else
                    results[j + 1] = string.char(1) .. string.rep('\0', 127) -- ERR_LINKED_EVENT_FAILED
                end
            end

            chain_start = nil
            modified_accounts = {}
            index_original_lengths = {}
        else
            table.insert(results, string.char(error_code) .. string.rep('\0', 127))
        end
    else
        -- Success - save state for potential rollback
        if chain_start ~= nil then
            if not modified_accounts[debit_account_id] then
                modified_accounts[debit_account_id] = debit_account
            end
            if not modified_accounts[credit_account_id] then
                modified_accounts[credit_account_id] = credit_account
            end
        end

        -- Commit changes
        local transfer_with_ts
        if is_imported then
            -- Use client-provided timestamp (must be non-zero)
            transfer_with_ts = transfer_data
        else
            -- Server sets timestamp
            transfer_with_ts = string.sub(transfer_data, 1, 120) .. ts_bytes
        end

        local debit_key = "account:" .. debit_account_id
        local credit_key = "account:" .. credit_account_id
        local transfer_key = "transfer:" .. transfer_id_hex

        redis.call('SET', debit_key, new_debit_account)
        redis.call('SET', credit_key, new_credit_account)
        redis.call('SET', transfer_key, transfer_with_ts)

        -- Track original lengths for potential rollback
        if chain_start ~= nil then
            local debit_transfers_key = "account:" .. debit_account_id .. ":transfers"
            local credit_transfers_key = "account:" .. credit_account_id .. ":transfers"

            if not index_original_lengths[debit_transfers_key] then
                index_original_lengths[debit_transfers_key] = redis.call('STRLEN', debit_transfers_key)
            end
            if not index_original_lengths[credit_transfers_key] then
                index_original_lengths[credit_transfers_key] = redis.call('STRLEN', credit_transfers_key)
            end
        end

        -- Add to transfer indexes (simple append, sorting done at query time)
        -- Append raw 16-byte transfer_id (fixed-size for easy rollback)
        redis.call('APPEND', "account:" .. debit_account_id .. ":transfers", transfer_id)
        redis.call('APPEND', "account:" .. credit_account_id .. ":transfers", transfer_id)

        -- Update balance history if accounts have HISTORY flag
        local ACCOUNT_FLAG_HISTORY = 0x08
        local debit_flags_val = string.byte(debit_account, 119) + string.byte(debit_account, 120) * 256
        local credit_flags_val = string.byte(credit_account, 119) + string.byte(credit_account, 120) * 256
        local debit_has_history = (math.floor(debit_flags_val / ACCOUNT_FLAG_HISTORY) % 2) == 1
        local credit_has_history = (math.floor(credit_flags_val / ACCOUNT_FLAG_HISTORY) % 2) == 1

        -- Helper: encode AccountBalance (128 bytes)
        local function encode_account_balance(account_data, transfer_ts)
            -- Extract timestamp from transfer_with_ts
            local ts_bytes = string.sub(transfer_with_ts, 121, 128)

            -- Extract balance fields from account (128 bytes)
            local debits_pending = string.sub(account_data, 17, 32)   -- offset 16, 16 bytes
            local debits_posted = string.sub(account_data, 33, 48)    -- offset 32, 16 bytes
            local credits_pending = string.sub(account_data, 49, 64)  -- offset 48, 16 bytes
            local credits_posted = string.sub(account_data, 65, 80)   -- offset 64, 16 bytes

            -- Return 128-byte AccountBalance: timestamp + balances + reserved (56 zero bytes)
            local reserved = string.rep('\0', 56)
            return ts_bytes .. debits_pending .. debits_posted .. credits_pending .. credits_posted .. reserved
        end

        if debit_has_history then
            -- Track original length for potential rollback
            if chain_start ~= nil then
                local debit_balance_key = "account:" .. debit_account_id .. ":balance_history"
                if not index_original_lengths[debit_balance_key] then
                    index_original_lengths[debit_balance_key] = redis.call('STRLEN', debit_balance_key)
                end
            end

            local debit_balance = encode_account_balance(new_debit_account, transfer_with_ts)
            redis.call('APPEND', "account:" .. debit_account_id .. ":balance_history", debit_balance)
        end

        if credit_has_history then
            -- Track original length for potential rollback
            if chain_start ~= nil then
                local credit_balance_key = "account:" .. credit_account_id .. ":balance_history"
                if not index_original_lengths[credit_balance_key] then
                    index_original_lengths[credit_balance_key] = redis.call('STRLEN', credit_balance_key)
                end
            end

            local credit_balance = encode_account_balance(new_credit_account, transfer_with_ts)
            redis.call('APPEND', "account:" .. credit_account_id .. ":balance_history", credit_balance)
        end

        table.insert(results, string.char(0) .. string.rep('\0', 127)) -- ERR_OK

        -- End chain if not linked
        if not is_linked and chain_start ~= nil then
            chain_start = nil
            modified_accounts = {}
            index_original_lengths = {}
        end
    end
end

-- Check for unclosed chain
if chain_start ~= nil then
    -- Rollback unclosed chain
    for j = chain_start, num_transfers - 1 do
        local rb_offset = j * 128 + 1
        local rb_id = extract_16bytes(transfers_data, rb_offset)
        local rb_id_hex = id_to_hex(rb_id)
        redis.call('DEL', "transfer:" .. rb_id_hex)

        local rb_debit_id = extract_16bytes(transfers_data, rb_offset + 16)
        local rb_credit_id = extract_16bytes(transfers_data, rb_offset + 32)

        if modified_accounts[rb_debit_id] then
            redis.call('SET', "account:" .. rb_debit_id, modified_accounts[rb_debit_id])
        end
        if modified_accounts[rb_credit_id] then
            redis.call('SET', "account:" .. rb_credit_id, modified_accounts[rb_credit_id])
        end
    end

    -- Rollback indexes by truncating to original lengths
    for key, original_len in pairs(index_original_lengths) do
        if original_len == 0 then
            redis.call('DEL', key)
        else
            -- Truncate by getting the prefix and setting it back
            local truncated = redis.call('GETRANGE', key, 0, original_len - 1)
            redis.call('SET', key, truncated)
        end
    end

    for j = chain_start, num_transfers - 1 do
        results[j + 1] = string.char(2) .. string.rep('\0', 127) -- ERR_LINKED_EVENT_CHAIN_OPEN
    end
end

-- Return concatenated results
return table.concat(results)
