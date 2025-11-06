-- Create Transfer Script (Binary Encoding)
-- Binary layout matches TigerBeetle format: 128 bytes fixed size
-- KEYS: none
-- ARGV[1]: transfer binary data (128 bytes)

-- Transfer binary layout (128 bytes total):
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

-- Account binary layout (128 bytes total):
-- id: 16 bytes (offset 0)
-- debits_pending: 16 bytes (offset 16)
-- debits_posted: 16 bytes (offset 32)
-- credits_pending: 16 bytes (offset 48)
-- credits_posted: 16 bytes (offset 64)
-- ... rest of fields

local transfer_data = ARGV[1]

-- Validate size
if #transfer_data ~= 128 then
    return lb_result(32) -- ERR_INVALID_DATA_SIZE
end

-- Extract fields from transfer
local transfer_id_raw = lb_slice16(transfer_data, 1)
local transfer_id = lb_hex16(transfer_id_raw)
local debit_account_id = lb_slice16(transfer_data, 17)
local credit_account_id = lb_slice16(transfer_data, 33)
local amount_bytes = lb_slice16(transfer_data, 49)
local flags = string.byte(transfer_data, 119) + string.byte(transfer_data, 120) * 256

local FLAG_LINKED = 0x0001
local FLAG_PENDING = 0x0002
local FLAG_POST_PENDING = 0x0004
local FLAG_VOID_PENDING = 0x0008
local FLAG_IMPORTED = 0x0100
local ACCOUNT_FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS = 0x0002
local ACCOUNT_FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS = 0x0004
local DEFAULT_TIMESTAMP = lb_encode_u64(1000000000000000000)
local BALANCE_RESERVED = string.rep('\0', 56)

-- Validate accounts are different
if debit_account_id == credit_account_id then
    return lb_result(40) -- ERR_ACCOUNTS_MUST_BE_DIFFERENT
end

-- Check if transfer exists
local transfer_key = "transfer:" .. transfer_id
if redis.call('EXISTS', transfer_key) == 1 then
    return lb_result(29) -- ERR_EXISTS_WITH_DIFFERENT_FLAGS
end

-- Load both accounts in one shot
local debit_key = "account:" .. debit_account_id
local credit_key = "account:" .. credit_account_id

local debit_account = redis.call('GET', debit_key)
local credit_account = redis.call('GET', credit_key)

if not debit_account or #debit_account ~= 128 then
    return lb_result(38) -- ERR_DEBIT_ACCOUNT_NOT_FOUND
end

if not credit_account or #credit_account ~= 128 then
    return lb_result(39) -- ERR_CREDIT_ACCOUNT_NOT_FOUND
end

-- Check ledgers match
local transfer_ledger = string.sub(transfer_data, 113, 116)
local debit_ledger = string.sub(debit_account, 113, 116)
local credit_ledger = string.sub(credit_account, 113, 116)

if transfer_ledger ~= debit_ledger or transfer_ledger ~= credit_ledger then
    return lb_result(52) -- ERR_LEDGER_MUST_MATCH
end

-- Check for LINKED flag (0x0001)
if lb_has_flag(flags, FLAG_LINKED) then
    return lb_result(1) -- ERR_LINKED_EVENT_CHAIN_OPEN
end

-- Parse flags
local is_pending = lb_has_flag(flags, FLAG_PENDING)
local is_post = lb_has_flag(flags, FLAG_POST_PENDING)
local is_void = lb_has_flag(flags, FLAG_VOID_PENDING)

-- Extract pending_id (offset 64, 16 bytes)
local pending_id_raw = lb_slice16(transfer_data, 65)
local pending_id = lb_hex16(pending_id_raw)

-- Update account balances
local new_debit_account = debit_account
local new_credit_account = credit_account

if is_post or is_void then
    -- Two-phase transfer resolution: lookup the pending transfer
    if pending_id_raw == lb_zero_16 then
        return lb_result(33) -- ERR_PENDING_ID_REQUIRED
    end

    local pending_transfer_key = "transfer:" .. pending_id
    local pending_transfer = redis.call('GET', pending_transfer_key)
    if not pending_transfer or #pending_transfer ~= 128 then
        return lb_result(34) -- ERR_PENDING_TRANSFER_NOT_FOUND
    end

    -- Extract pending transfer details
    local pending_flags = string.byte(pending_transfer, 119) + string.byte(pending_transfer, 120) * 256
    local pending_is_pending = lb_has_flag(pending_flags, FLAG_PENDING)

    if not pending_is_pending then
        return lb_result(35) -- ERR_PENDING_TRANSFER_NOT_PENDING
    end

    local pending_debit_id = lb_slice16(pending_transfer, 17)
    local pending_credit_id = lb_slice16(pending_transfer, 33)
    local pending_amount_bytes = lb_slice16(pending_transfer, 49)

    if pending_debit_id ~= debit_account_id or pending_credit_id ~= credit_account_id then
        return lb_result(34) -- ERR_PENDING_TRANSFER_NOT_FOUND
    end

    if pending_amount_bytes ~= amount_bytes then
        return lb_result(34) -- ERR_PENDING_TRANSFER_NOT_FOUND
    end

    if is_post then
        -- POST_PENDING_TRANSFER: move from pending to posted
        -- Reduce pending balances
        local debit_pending = lb_sub_field(debit_account, 17, pending_amount_bytes)
        if not debit_pending then
            return lb_result(35) -- ERR_PENDING_TRANSFER_ALREADY_POSTED
        end
        local credit_pending = lb_sub_field(credit_account, 49, pending_amount_bytes)
        if not credit_pending then
            return lb_result(35)
        end

        -- Increase posted balances
        local debit_posted = lb_add_field(debit_account, 33, pending_amount_bytes)
        if not debit_posted then
            return lb_result(42) -- ERR_EXCEEDS_CREDITS
        end
        local credit_posted = lb_add_field(credit_account, 65, pending_amount_bytes)
        if not credit_posted then
            return lb_result(43) -- ERR_EXCEEDS_DEBITS
        end

        new_debit_account = string.sub(debit_account, 1, 16) .. debit_pending .. debit_posted .. string.sub(debit_account, 49, 128)
        new_credit_account = string.sub(credit_account, 1, 48) .. credit_pending .. credit_posted .. string.sub(credit_account, 81, 128)
    elseif is_void then
        -- VOID_PENDING_TRANSFER: return funds by reducing pending balances
        local debit_pending = lb_sub_field(debit_account, 17, pending_amount_bytes)
        if not debit_pending then
            return lb_result(36) -- ERR_PENDING_TRANSFER_ALREADY_VOIDED
        end
        local credit_pending = lb_sub_field(credit_account, 49, pending_amount_bytes)
        if not credit_pending then
            return lb_result(36)
        end

        new_debit_account = string.sub(debit_account, 1, 16) .. debit_pending .. string.sub(debit_account, 33, 128)
        new_credit_account = string.sub(credit_account, 1, 48) .. credit_pending .. string.sub(credit_account, 65, 128)
    end
elseif is_pending then
    -- Phase 1: Create pending transfer
    -- Update debits_pending (offset 16) and credits_pending (offset 48)
    local debit_pending = lb_add_field(debit_account, 17, amount_bytes)
    if not debit_pending then
        return lb_result(42)
    end
    local credit_pending = lb_add_field(credit_account, 49, amount_bytes)
    if not credit_pending then
        return lb_result(43)
    end

    new_debit_account = string.sub(debit_account, 1, 16) .. debit_pending .. string.sub(debit_account, 33, 128)
    new_credit_account = string.sub(credit_account, 1, 48) .. credit_pending .. string.sub(credit_account, 65, 128)
else
    -- Direct posted transfer: update debits_posted (offset 32) and credits_posted (offset 64)
    local debit_posted = lb_add_field(debit_account, 33, amount_bytes)
    if not debit_posted then
        return lb_result(42)
    end
    local credit_posted = lb_add_field(credit_account, 65, amount_bytes)
    if not credit_posted then
        return lb_result(43)
    end

    new_debit_account = string.sub(debit_account, 1, 32) .. debit_posted .. string.sub(debit_account, 49, 128)
    new_credit_account = string.sub(credit_account, 1, 64) .. credit_posted .. string.sub(credit_account, 81, 128)
end

-- Check balance constraints
local debit_flags = string.byte(debit_account, 119) + string.byte(debit_account, 120) * 256
local credit_flags = string.byte(credit_account, 119) + string.byte(credit_account, 120) * 256

local ACCOUNT_FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS = 0x0002
local ACCOUNT_FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS = 0x0004

if lb_has_flag(debit_flags, ACCOUNT_FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS) then
    local debit_posted_bytes = lb_slice16(new_debit_account, 33)
    local debit_pending_bytes = lb_slice16(new_debit_account, 17)
    local debit_total, debit_overflow = lb_add_u128(debit_posted_bytes, debit_pending_bytes)
    if debit_overflow ~= 0 then
        return lb_result(42)
    end
    local credit_posted_bytes = lb_slice16(new_debit_account, 65)
    local credit_pending_bytes = lb_slice16(new_debit_account, 49)
    local credit_total, credit_overflow = lb_add_u128(credit_posted_bytes, credit_pending_bytes)
    if credit_overflow ~= 0 then
        return lb_result(43)
    end
    if lb_compare_u128(debit_total, credit_total) == 1 then
        return lb_result(42) -- ERR_EXCEEDS_CREDITS
    end
end

if lb_has_flag(credit_flags, ACCOUNT_FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS) then
    local credit_posted_bytes = lb_slice16(new_credit_account, 65)
    local credit_pending_bytes = lb_slice16(new_credit_account, 49)
    local credit_total, credit_overflow = lb_add_u128(credit_posted_bytes, credit_pending_bytes)
    if credit_overflow ~= 0 then
        return lb_result(43)
    end
    local debit_posted_bytes = lb_slice16(new_credit_account, 33)
    local debit_pending_bytes = lb_slice16(new_credit_account, 17)
    local debit_total, debit_overflow = lb_add_u128(debit_posted_bytes, debit_pending_bytes)
    if debit_overflow ~= 0 then
        return lb_result(42)
    end
    if lb_compare_u128(credit_total, debit_total) == 1 then
        return lb_result(43) -- ERR_EXCEEDS_DEBITS
    end
end

local transfer_with_ts
if not lb_has_flag(flags, FLAG_IMPORTED) then
    -- imported flag is NOT set, server sets timestamp
    transfer_with_ts = string.sub(transfer_data, 1, 120) .. DEFAULT_TIMESTAMP
else
    -- imported flag IS set, use client-provided timestamp (must be non-zero)
    local ts = lb_decode_u64(transfer_data, 121)

    -- Timestamp must be non-zero when imported flag is set
    if ts == 0 then
        return lb_result(32) -- ERR_INVALID_DATA_SIZE
    end

    transfer_with_ts = transfer_data
end

-- Write accounts and transfer
redis.call('SET', debit_key, new_debit_account)
redis.call('SET', credit_key, new_credit_account)
redis.call('SET', transfer_key, transfer_with_ts)

-- Add to transfer indexes (simple append, sorting done at query time)
-- Append raw 16-byte transfer_id (fixed-size for easy rollback)
local debit_index = "account:" .. debit_account_id .. ":transfers"
local credit_index = "account:" .. credit_account_id .. ":transfers"
redis.call('APPEND', debit_index, transfer_id_raw)
redis.call('APPEND', credit_index, transfer_id_raw)

-- Update balance history if accounts have HISTORY flag
local ACCOUNT_FLAG_HISTORY = 0x08
local debit_has_history = lb_has_flag(debit_flags, ACCOUNT_FLAG_HISTORY)
local credit_has_history = lb_has_flag(credit_flags, ACCOUNT_FLAG_HISTORY)

-- Helper: encode AccountBalance (128 bytes)
local function encode_account_balance(account_data)
    -- Extract timestamp from transfer_with_ts
    local ts_bytes = string.sub(transfer_with_ts, 121, 128)

    -- Extract balance fields from account (128 bytes)
    local debits_pending = lb_slice16(account_data, 17)   -- offset 16, 16 bytes
    local debits_posted = lb_slice16(account_data, 33)    -- offset 32, 16 bytes
    local credits_pending = lb_slice16(account_data, 49)  -- offset 48, 16 bytes
    local credits_posted = lb_slice16(account_data, 65)   -- offset 64, 16 bytes

    -- Return 128-byte AccountBalance: timestamp + balances + reserved (56 bytes of zeros)
    return ts_bytes .. debits_pending .. debits_posted .. credits_pending .. credits_posted .. BALANCE_RESERVED
end

if debit_has_history then
    local debit_balance = encode_account_balance(new_debit_account)
    local debit_balance_index = "account:" .. debit_account_id .. ":balance_history"
    redis.call('APPEND', debit_balance_index, debit_balance)
end

if credit_has_history then
    local credit_balance = encode_account_balance(new_credit_account)
    local credit_balance_index = "account:" .. credit_account_id .. ":balance_history"
    redis.call('APPEND', credit_balance_index, credit_balance)
end

-- Return success
return lb_result(0)
