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

local ERR_INVALID_DATA_SIZE = 32
local ERR_LINKED_EVENT_FAILED = 1
local ERR_LINKED_EVENT_CHAIN_OPEN = 2
local ERR_RESERVED_FLAG = 4
local ERR_EXISTS = 46
local ERR_DEBIT_ACCOUNT_NOT_FOUND = 21
local ERR_CREDIT_ACCOUNT_NOT_FOUND = 22
local ERR_ACCOUNTS_MUST_BE_DIFFERENT = 12
local ERR_ACCOUNTS_MUST_HAVE_THE_SAME_LEDGER = 23
local ERR_TRANSFER_MUST_HAVE_THE_SAME_LEDGER_AS_ACCOUNTS = 24
local ERR_PENDING_ID_MUST_NOT_BE_ZERO = 14
local ERR_PENDING_TRANSFER_NOT_FOUND = 25
local ERR_PENDING_TRANSFER_NOT_PENDING = 26
local ERR_PENDING_TRANSFER_HAS_DIFFERENT_DEBIT_ACCOUNT_ID = 27
local ERR_PENDING_TRANSFER_HAS_DIFFERENT_CREDIT_ACCOUNT_ID = 28
local ERR_PENDING_TRANSFER_HAS_DIFFERENT_LEDGER = 29
local ERR_PENDING_TRANSFER_HAS_DIFFERENT_CODE = 30
local ERR_PENDING_TRANSFER_ALREADY_POSTED = 33
local ERR_PENDING_TRANSFER_ALREADY_VOIDED = 34
local ERR_OVERFLOWS_DEBITS_PENDING = 47
local ERR_OVERFLOWS_CREDITS_PENDING = 48
local ERR_OVERFLOWS_DEBITS_POSTED = 49
local ERR_OVERFLOWS_CREDITS_POSTED = 50
local ERR_EXCEEDS_CREDITS = 54
local ERR_EXCEEDS_DEBITS = 55

-- Validate size is multiple of 128
if data_len % 128 ~= 0 then
    return lb_result(ERR_INVALID_DATA_SIZE)
end

local num_transfers = data_len / 128

local ZERO_ID = lb_zero_16

-- Get timestamp once (for non-imported transfers)
local timestamp = redis.call('TIME')
local ts_bytes = lb_encode_u64(tonumber(timestamp[1]) * 1000000000 + tonumber(timestamp[2]) * 1000)

local results = {}
local chain_start = nil
local modified_accounts = {}
local index_original_lengths = {} -- Track original lengths for rollback
local added_transfer_members = {}

local FLAG_PENDING = 0x0002
local FLAG_POST_PENDING = 0x0004
local FLAG_VOID_PENDING = 0x0008
local FLAG_BALANCING_DEBIT = 0x0010
local FLAG_BALANCING_CREDIT = 0x0020
local FLAG_CLOSING_DEBIT = 0x0040
local FLAG_CLOSING_CREDIT = 0x0080
local FLAG_IMPORTED = 0x0100
local ACCOUNT_FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS = 0x0002
local ACCOUNT_FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS = 0x0004
local BALANCE_RESERVED = string.rep('\0', 56)

local FLAG_LINKED = 0x0001

for i = 0, num_transfers - 1 do
    local offset = i * 128 + 1
    local transfer_data = string.sub(transfers_data, offset, offset + 127)

    -- Extract fields
    local transfer_id = lb_slice16(transfer_data, 1)
    local transfer_id_hex = lb_hex16(transfer_id)
    local debit_account_id = lb_slice16(transfer_data, 17)
    local credit_account_id = lb_slice16(transfer_data, 33)
    local amount_bytes = lb_slice16(transfer_data, 49)
    local flags = string.byte(transfer_data, 119) + string.byte(transfer_data, 120) * 256
    local is_linked = lb_has_flag(flags, FLAG_LINKED)
    local is_pending = lb_has_flag(flags, FLAG_PENDING)
    local is_post = lb_has_flag(flags, FLAG_POST_PENDING)
    local is_void = lb_has_flag(flags, FLAG_VOID_PENDING)
    local is_imported = lb_has_flag(flags, FLAG_IMPORTED)

    local error_code = 0

    if is_imported then
        -- Imported-event semantics are not implemented in lua_beetle.
        error_code = ERR_RESERVED_FLAG
    end

    if error_code == 0 and (
        lb_has_flag(flags, FLAG_BALANCING_DEBIT) or
        lb_has_flag(flags, FLAG_BALANCING_CREDIT) or
        lb_has_flag(flags, FLAG_CLOSING_DEBIT) or
        lb_has_flag(flags, FLAG_CLOSING_CREDIT)) then
        error_code = ERR_RESERVED_FLAG
    end

    -- Start chain tracking
    if chain_start == nil and is_linked then
        chain_start = i
    end

    -- Validate accounts are different
    if debit_account_id == credit_account_id then
        error_code = ERR_ACCOUNTS_MUST_BE_DIFFERENT
    end

    -- Check if transfer exists
    if error_code == 0 then
        local transfer_key = "transfer:" .. transfer_id_hex
        if redis.call('EXISTS', transfer_key) == 1 then
            error_code = ERR_EXISTS
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
            error_code = ERR_DEBIT_ACCOUNT_NOT_FOUND
        elseif not credit_account or #credit_account ~= 128 then
            error_code = ERR_CREDIT_ACCOUNT_NOT_FOUND
        end
    end

    -- Validate and process transfer
    local new_debit_account, new_credit_account
    if error_code == 0 then
        -- Check ledgers match
        local transfer_ledger = lb_decode_u32(transfer_data, 113)
        local debit_ledger = lb_decode_u32(debit_account, 113)
        local credit_ledger = lb_decode_u32(credit_account, 113)

        if debit_ledger ~= credit_ledger then
            error_code = ERR_ACCOUNTS_MUST_HAVE_THE_SAME_LEDGER
        elseif transfer_ledger ~= debit_ledger then
            error_code = ERR_TRANSFER_MUST_HAVE_THE_SAME_LEDGER_AS_ACCOUNTS
        else
            new_debit_account = debit_account
            new_credit_account = credit_account

            local pending_id_raw = lb_slice16(transfer_data, 65)
            local pending_id_hex = lb_hex16(pending_id_raw)

            if is_post or is_void then
                if pending_id_raw == ZERO_ID then
                    error_code = ERR_PENDING_ID_MUST_NOT_BE_ZERO
                else
                    local pending_transfer_key = "transfer:" .. pending_id_hex
                    local pending_transfer = redis.call('GET', pending_transfer_key)
                    if not pending_transfer or #pending_transfer ~= 128 then
                        error_code = ERR_PENDING_TRANSFER_NOT_FOUND
                    else
                        local pending_flags = string.byte(pending_transfer, 119) + string.byte(pending_transfer, 120) * 256
                        if not lb_has_flag(pending_flags, FLAG_PENDING) then
                            error_code = ERR_PENDING_TRANSFER_NOT_PENDING
                        else
                            local pending_debit_id = lb_slice16(pending_transfer, 17)
                            local pending_credit_id = lb_slice16(pending_transfer, 33)
                            local pending_amount_bytes = lb_slice16(pending_transfer, 49)

                            if pending_debit_id ~= debit_account_id then
                                error_code = ERR_PENDING_TRANSFER_HAS_DIFFERENT_DEBIT_ACCOUNT_ID
                            elseif pending_credit_id ~= credit_account_id then
                                error_code = ERR_PENDING_TRANSFER_HAS_DIFFERENT_CREDIT_ACCOUNT_ID
                            elseif lb_decode_u32(pending_transfer, 113) ~= transfer_ledger then
                                error_code = ERR_PENDING_TRANSFER_HAS_DIFFERENT_LEDGER
                            elseif lb_decode_u16(pending_transfer, 117) ~= lb_decode_u16(transfer_data, 117) then
                                error_code = ERR_PENDING_TRANSFER_HAS_DIFFERENT_CODE
                            elseif pending_amount_bytes ~= amount_bytes then
                                error_code = is_void and 32 or ERR_PENDING_TRANSFER_NOT_FOUND
                            elseif is_post then
                                local debit_pending = lb_sub_field(debit_account, 17, pending_amount_bytes)
                                if not debit_pending then
                                    error_code = ERR_PENDING_TRANSFER_ALREADY_POSTED
                                else
                                    local credit_pending = lb_sub_field(credit_account, 49, pending_amount_bytes)
                                    if not credit_pending then
                                        error_code = ERR_PENDING_TRANSFER_ALREADY_POSTED
                                    else
                                        local debit_posted = lb_add_field(debit_account, 33, pending_amount_bytes)
                                        if not debit_posted then
                                            error_code = ERR_OVERFLOWS_DEBITS_POSTED
                                        else
                                            local credit_posted = lb_add_field(credit_account, 65, pending_amount_bytes)
                                            if not credit_posted then
                                                error_code = ERR_OVERFLOWS_CREDITS_POSTED
                                            else
                                                new_debit_account = string.sub(debit_account, 1, 16) .. debit_pending .. debit_posted .. string.sub(debit_account, 49, 128)
                                                new_credit_account = string.sub(credit_account, 1, 48) .. credit_pending .. credit_posted .. string.sub(credit_account, 81, 128)
                                            end
                                        end
                                    end
                                end
                            else
                                local debit_pending = lb_sub_field(debit_account, 17, pending_amount_bytes)
                                if not debit_pending then
                                    error_code = ERR_PENDING_TRANSFER_ALREADY_VOIDED
                                else
                                    local credit_pending = lb_sub_field(credit_account, 49, pending_amount_bytes)
                                    if not credit_pending then
                                        error_code = ERR_PENDING_TRANSFER_ALREADY_VOIDED
                                    else
                                        new_debit_account = string.sub(debit_account, 1, 16) .. debit_pending .. string.sub(debit_account, 33, 128)
                                        new_credit_account = string.sub(credit_account, 1, 48) .. credit_pending .. string.sub(credit_account, 65, 128)
                                    end
                                end
                            end
                        end
                    end
                end
            elseif is_pending then
                local debit_pending = lb_add_field(debit_account, 17, amount_bytes)
                if not debit_pending then
                    error_code = ERR_OVERFLOWS_DEBITS_PENDING
                else
                    local credit_pending = lb_add_field(credit_account, 49, amount_bytes)
                    if not credit_pending then
                        error_code = ERR_OVERFLOWS_CREDITS_PENDING
                    else
                        new_debit_account = string.sub(debit_account, 1, 16) .. debit_pending .. string.sub(debit_account, 33, 128)
                        new_credit_account = string.sub(credit_account, 1, 48) .. credit_pending .. string.sub(credit_account, 65, 128)
                    end
                end
            else
                local debit_posted = lb_add_field(debit_account, 33, amount_bytes)
                if not debit_posted then
                    error_code = ERR_OVERFLOWS_DEBITS_POSTED
                else
                    local credit_posted = lb_add_field(credit_account, 65, amount_bytes)
                    if not credit_posted then
                        error_code = ERR_OVERFLOWS_CREDITS_POSTED
                    else
                        new_debit_account = string.sub(debit_account, 1, 32) .. debit_posted .. string.sub(debit_account, 49, 128)
                        new_credit_account = string.sub(credit_account, 1, 64) .. credit_posted .. string.sub(credit_account, 81, 128)
                    end
                end
            end

            if error_code == 0 then
                local debit_flags = string.byte(new_debit_account, 119) + string.byte(new_debit_account, 120) * 256
                local credit_flags = string.byte(new_credit_account, 119) + string.byte(new_credit_account, 120) * 256

                if lb_has_flag(debit_flags, ACCOUNT_FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS) then
                    local debit_posted_bytes = lb_slice16(new_debit_account, 33)
                    local debit_pending_bytes = lb_slice16(new_debit_account, 17)
                    local debit_total, debit_overflow = lb_add_u128(debit_posted_bytes, debit_pending_bytes)
                    if debit_overflow ~= 0 then
                        error_code = ERR_EXCEEDS_CREDITS
                    else
                        local credit_posted_bytes = lb_slice16(new_debit_account, 65)
                        local credit_pending_bytes = lb_slice16(new_debit_account, 49)
                        local credit_total, credit_overflow = lb_add_u128(credit_posted_bytes, credit_pending_bytes)
                        if credit_overflow ~= 0 then
                            error_code = ERR_EXCEEDS_DEBITS
                        elseif lb_compare_u128(debit_total, credit_total) == 1 then
                            error_code = ERR_EXCEEDS_CREDITS
                        end
                    end
                end

                if error_code == 0 and lb_has_flag(credit_flags, ACCOUNT_FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS) then
                    local credit_posted_bytes = lb_slice16(new_credit_account, 65)
                    local credit_pending_bytes = lb_slice16(new_credit_account, 49)
                    local credit_total, credit_overflow = lb_add_u128(credit_posted_bytes, credit_pending_bytes)
                    if credit_overflow ~= 0 then
                        error_code = ERR_EXCEEDS_DEBITS
                    else
                        local debit_posted_bytes = lb_slice16(new_credit_account, 33)
                        local debit_pending_bytes = lb_slice16(new_credit_account, 17)
                        local debit_total, debit_overflow = lb_add_u128(debit_posted_bytes, debit_pending_bytes)
                        if debit_overflow ~= 0 then
                            error_code = ERR_EXCEEDS_CREDITS
                        elseif lb_compare_u128(credit_total, debit_total) == 1 then
                            error_code = ERR_EXCEEDS_DEBITS
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
                local rb_id = lb_slice16(transfers_data, rb_offset)
                local rb_id_hex = lb_hex16(rb_id)
                redis.call('DEL', "transfer:" .. rb_id_hex)

                -- Restore modified accounts
                local rb_debit_id = lb_slice16(transfers_data, rb_offset + 16)
                local rb_credit_id = lb_slice16(transfers_data, rb_offset + 32)

                if modified_accounts[rb_debit_id] then
                    redis.call('SET', "account:" .. rb_debit_id, modified_accounts[rb_debit_id])
                end
                if modified_accounts[rb_credit_id] then
                    redis.call('SET', "account:" .. rb_credit_id, modified_accounts[rb_credit_id])
                end
            end

            -- Rollback indexes by restoring previous collection sizes.
            for key, original_len in pairs(index_original_lengths) do
                if string.sub(key, -10) == ":transfers" then
                    local members = added_transfer_members[key]
                    if members then
                        for _, member in ipairs(members) do
                            redis.call('ZREM', key, member)
                        end
                    end
                else
                    if original_len == 0 then
                        redis.call('DEL', key)
                    else
                        redis.call('LTRIM', key, 0, original_len - 1)
                    end
                end
            end

            -- Mark all in chain as failed
            for j = chain_start, i do
                if j == i then
                    results[j + 1] = lb_result(error_code)
                else
                    results[j + 1] = lb_result(ERR_LINKED_EVENT_FAILED)
                end
            end

            chain_start = nil
            modified_accounts = {}
            index_original_lengths = {}
            added_transfer_members = {}
        else
            results[#results + 1] = lb_result(error_code)
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
        transfer_with_ts = string.sub(transfer_data, 1, 120) .. ts_bytes

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
                index_original_lengths[debit_transfers_key] = redis.call('ZCARD', debit_transfers_key)
            end
            if not index_original_lengths[credit_transfers_key] then
                index_original_lengths[credit_transfers_key] = redis.call('ZCARD', credit_transfers_key)
            end
        end

        -- Add to transfer indexes using the same format as EloqKV:
        -- sorted set member = 32-char hex transfer id, score = timestamp in microseconds
        local transfer_timestamp = lb_decode_u64(transfer_with_ts, 121)
        local transfer_score = tostring(math.floor(transfer_timestamp / 1000))
        local debit_transfers_key = "account:" .. debit_account_id .. ":transfers"
        local credit_transfers_key = "account:" .. credit_account_id .. ":transfers"
        redis.call('ZADD', debit_transfers_key, transfer_score, transfer_id_hex)
        redis.call('ZADD', credit_transfers_key, transfer_score, transfer_id_hex)
        if chain_start ~= nil then
            if not added_transfer_members[debit_transfers_key] then
                added_transfer_members[debit_transfers_key] = {}
            end
            table.insert(added_transfer_members[debit_transfers_key], transfer_id_hex)
            if not added_transfer_members[credit_transfers_key] then
                added_transfer_members[credit_transfers_key] = {}
            end
            table.insert(added_transfer_members[credit_transfers_key], transfer_id_hex)
        end

        -- Update balance history if accounts have HISTORY flag
        local ACCOUNT_FLAG_HISTORY = 0x08
        local debit_flags_val = string.byte(debit_account, 119) + string.byte(debit_account, 120) * 256
        local credit_flags_val = string.byte(credit_account, 119) + string.byte(credit_account, 120) * 256
        local debit_has_history = lb_has_flag(debit_flags_val, ACCOUNT_FLAG_HISTORY)
        local credit_has_history = lb_has_flag(credit_flags_val, ACCOUNT_FLAG_HISTORY)

        -- Helper: encode AccountBalance (128 bytes)
        local function encode_account_balance(account_data)
            -- Extract timestamp from transfer_with_ts
            local ts_bytes = string.sub(transfer_with_ts, 121, 128)

            -- Extract balance fields from account (128 bytes)
            local debits_pending = lb_slice16(account_data, 17)   -- offset 16, 16 bytes
            local debits_posted = lb_slice16(account_data, 33)    -- offset 32, 16 bytes
            local credits_pending = lb_slice16(account_data, 49)  -- offset 48, 16 bytes
            local credits_posted = lb_slice16(account_data, 65)   -- offset 64, 16 bytes

            -- Return 128-byte AccountBalance in current TigerBeetle field order.
            return debits_pending .. debits_posted .. credits_pending .. credits_posted .. ts_bytes .. BALANCE_RESERVED
        end

        if debit_has_history then
            -- Track original length for potential rollback
            if chain_start ~= nil then
                local debit_balance_key = "account:" .. debit_account_id .. ":balance_history"
                if not index_original_lengths[debit_balance_key] then
                    index_original_lengths[debit_balance_key] = redis.call('LLEN', debit_balance_key)
                end
            end

            local debit_balance = encode_account_balance(new_debit_account)
            redis.call('RPUSH', "account:" .. debit_account_id .. ":balance_history", debit_balance)
        end

        if credit_has_history then
            -- Track original length for potential rollback
            if chain_start ~= nil then
                local credit_balance_key = "account:" .. credit_account_id .. ":balance_history"
                if not index_original_lengths[credit_balance_key] then
                    index_original_lengths[credit_balance_key] = redis.call('LLEN', credit_balance_key)
                end
            end

            local credit_balance = encode_account_balance(new_credit_account)
            redis.call('RPUSH', "account:" .. credit_account_id .. ":balance_history", credit_balance)
        end

        results[#results + 1] = lb_result(0) -- ERR_OK

        -- End chain if not linked
        if not is_linked and chain_start ~= nil then
            chain_start = nil
            modified_accounts = {}
            index_original_lengths = {}
            added_transfer_members = {}
        end
    end
end

-- Check for unclosed chain
if chain_start ~= nil then
    -- Rollback unclosed chain
    for j = chain_start, num_transfers - 1 do
        local rb_offset = j * 128 + 1
        local rb_id = lb_slice16(transfers_data, rb_offset)
        local rb_id_hex = lb_hex16(rb_id)
        redis.call('DEL', "transfer:" .. rb_id_hex)

        local rb_debit_id = lb_slice16(transfers_data, rb_offset + 16)
        local rb_credit_id = lb_slice16(transfers_data, rb_offset + 32)

        if modified_accounts[rb_debit_id] then
            redis.call('SET', "account:" .. rb_debit_id, modified_accounts[rb_debit_id])
        end
        if modified_accounts[rb_credit_id] then
            redis.call('SET', "account:" .. rb_credit_id, modified_accounts[rb_credit_id])
        end
    end

    -- Rollback indexes by restoring previous collection sizes.
    for key, original_len in pairs(index_original_lengths) do
        if string.sub(key, -10) == ":transfers" then
            local members = added_transfer_members[key]
            if members then
                for _, member in ipairs(members) do
                    redis.call('ZREM', key, member)
                end
            end
        else
            if original_len == 0 then
                redis.call('DEL', key)
            else
                redis.call('LTRIM', key, 0, original_len - 1)
            end
        end
    end

    for j = chain_start, num_transfers - 1 do
        if j == num_transfers - 1 then
            results[j + 1] = lb_result(ERR_LINKED_EVENT_CHAIN_OPEN)
        else
            results[j + 1] = lb_result(ERR_LINKED_EVENT_FAILED)
        end
    end
end

-- Return concatenated results
return table.concat(results)
