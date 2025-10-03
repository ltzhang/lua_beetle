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
    return string.char(32) .. string.rep('\0', 127) -- ERR_INVALID_DATA_SIZE = 32
end

-- Helper to extract 16-byte uint as string (for comparison)
local function extract_16bytes(data, offset)
    return string.sub(data, offset, offset + 15)
end

-- Helper to convert 16-byte ID to hex string for Redis keys
local function id_to_string(id_bytes)
    local hex = ""
    for i = 1, #id_bytes do
        hex = hex .. string.format("%02x", string.byte(id_bytes, i))
    end
    return hex
end

-- Helper to extract and decode little-endian uint128
local function decode_u128(data, offset)
    local result = 0
    local multiplier = 1
    for i = 0, 15 do
        result = result + string.byte(data, offset + i) * multiplier
        multiplier = multiplier * 256
        if multiplier > 1e15 then break end -- Lua number precision limit
    end
    return result
end

-- Helper to encode little-endian uint128
local function encode_u128(value)
    local bytes = {}
    for i = 1, 16 do
        bytes[i] = string.char(value % 256)
        value = math.floor(value / 256)
    end
    return table.concat(bytes)
end

-- Helper to add two u128 values (as binary strings)
local function add_u128(a_data, a_offset, b_data, b_offset)
    local a_val = decode_u128(a_data, a_offset)
    local b_val = decode_u128(b_data, b_offset)
    return encode_u128(a_val + b_val)
end

-- Helper to subtract b from a (as binary strings)
local function sub_u128(a_data, a_offset, b_data, b_offset)
    local a_val = decode_u128(a_data, a_offset)
    local b_val = decode_u128(b_data, b_offset)
    if a_val < b_val then
        return nil -- Underflow
    end
    return encode_u128(a_val - b_val)
end

-- Extract fields from transfer
local transfer_id_raw = extract_16bytes(transfer_data, 1)
local transfer_id = id_to_string(transfer_id_raw)
local debit_account_id = extract_16bytes(transfer_data, 17)
local credit_account_id = extract_16bytes(transfer_data, 33)
local amount_bytes = extract_16bytes(transfer_data, 49)
local flags = string.byte(transfer_data, 119) + string.byte(transfer_data, 120) * 256

-- Validate accounts are different
if debit_account_id == credit_account_id then
    return string.char(40) .. string.rep('\0', 127) -- ERR_ACCOUNTS_MUST_BE_DIFFERENT = 40
end

-- Check if transfer exists
local transfer_key = "transfer:" .. transfer_id
if redis.call('EXISTS', transfer_key) == 1 then
    return string.char(29) .. string.rep('\0', 127) -- ERR_EXISTS_WITH_DIFFERENT_FLAGS = 29
end

-- Load both accounts in one shot
local debit_key = "account:" .. debit_account_id
local credit_key = "account:" .. credit_account_id

local debit_account = redis.call('GET', debit_key)
local credit_account = redis.call('GET', credit_key)

if not debit_account or #debit_account ~= 128 then
    return string.char(38) .. string.rep('\0', 127) -- ERR_DEBIT_ACCOUNT_NOT_FOUND = 38
end

if not credit_account or #credit_account ~= 128 then
    return string.char(39) .. string.rep('\0', 127) -- ERR_CREDIT_ACCOUNT_NOT_FOUND = 39
end

-- Check ledgers match
local transfer_ledger = string.sub(transfer_data, 113, 116)
local debit_ledger = string.sub(debit_account, 113, 116)
local credit_ledger = string.sub(credit_account, 113, 116)

if transfer_ledger ~= debit_ledger or transfer_ledger ~= credit_ledger then
    return string.char(52) .. string.rep('\0', 127) -- ERR_LEDGER_MUST_MATCH = 52
end

-- Check for LINKED flag (0x0001)
if (flags % 2) == 1 then
    return string.char(1) .. string.rep('\0', 127) -- ERR_LINKED_EVENT_CHAIN_OPEN = 1
end

-- Parse flags
local FLAG_PENDING = 0x0002
local FLAG_POST_PENDING = 0x0004
local FLAG_VOID_PENDING = 0x0008

local is_pending = (math.floor(flags / FLAG_PENDING) % 2) == 1
local is_post = (math.floor(flags / FLAG_POST_PENDING) % 2) == 1
local is_void = (math.floor(flags / FLAG_VOID_PENDING) % 2) == 1

-- Extract pending_id (offset 64, 16 bytes)
local pending_id_raw = extract_16bytes(transfer_data, 65)
local pending_id = id_to_string(pending_id_raw)

-- Update account balances
local new_debit_account = debit_account
local new_credit_account = credit_account

if is_post or is_void then
    -- Two-phase transfer resolution: lookup the pending transfer
    if pending_id == "00000000000000000000000000000000" then
        return string.char(33) .. string.rep('\0', 127) -- ERR_PENDING_ID_REQUIRED
    end

    local pending_transfer_key = "transfer:" .. pending_id
    local pending_transfer = redis.call('GET', pending_transfer_key)
    if not pending_transfer then
        return string.char(34) .. string.rep('\0', 127) -- ERR_PENDING_TRANSFER_NOT_FOUND
    end

    -- Extract pending transfer details
    local pending_flags = string.byte(pending_transfer, 119) + string.byte(pending_transfer, 120) * 256
    local pending_is_pending = (math.floor(pending_flags / FLAG_PENDING) % 2) == 1

    if not pending_is_pending then
        return string.char(35) .. string.rep('\0', 127) -- ERR_PENDING_TRANSFER_NOT_PENDING
    end

    -- Get pending transfer amount
    local pending_amount_bytes = extract_16bytes(pending_transfer, 49)

    if is_post then
        -- POST_PENDING_TRANSFER: move from pending to posted
        -- Reduce pending balances
        local debit_pending = sub_u128(debit_account, 17, pending_amount_bytes, 1)
        local credit_pending = sub_u128(credit_account, 49, pending_amount_bytes, 1)

        -- Increase posted balances
        local debit_posted = add_u128(debit_account, 33, pending_amount_bytes, 1)
        local credit_posted = add_u128(credit_account, 65, pending_amount_bytes, 1)

        new_debit_account = string.sub(debit_account, 1, 16) .. debit_pending .. debit_posted .. string.sub(debit_account, 49, 128)
        new_credit_account = string.sub(credit_account, 1, 48) .. credit_pending .. credit_posted .. string.sub(credit_account, 81, 128)
    elseif is_void then
        -- VOID_PENDING_TRANSFER: return funds by reducing pending balances
        local debit_pending = sub_u128(debit_account, 17, pending_amount_bytes, 1)
        local credit_pending = sub_u128(credit_account, 49, pending_amount_bytes, 1)

        new_debit_account = string.sub(debit_account, 1, 16) .. debit_pending .. string.sub(debit_account, 33, 128)
        new_credit_account = string.sub(credit_account, 1, 48) .. credit_pending .. string.sub(credit_account, 65, 128)
    end
elseif is_pending then
    -- Phase 1: Create pending transfer
    -- Update debits_pending (offset 16) and credits_pending (offset 48)
    local debit_pending = add_u128(debit_account, 17, amount_bytes, 1)
    local credit_pending = add_u128(credit_account, 49, amount_bytes, 1)

    new_debit_account = string.sub(debit_account, 1, 16) .. debit_pending .. string.sub(debit_account, 33, 128)
    new_credit_account = string.sub(credit_account, 1, 48) .. credit_pending .. string.sub(credit_account, 65, 128)
else
    -- Direct posted transfer: update debits_posted (offset 32) and credits_posted (offset 64)
    local debit_posted = add_u128(debit_account, 33, amount_bytes, 1)
    local credit_posted = add_u128(credit_account, 65, amount_bytes, 1)

    new_debit_account = string.sub(debit_account, 1, 32) .. debit_posted .. string.sub(debit_account, 49, 128)
    new_credit_account = string.sub(credit_account, 1, 64) .. credit_posted .. string.sub(credit_account, 81, 128)
end

-- Check balance constraints
local debit_flags = string.byte(debit_account, 119) + string.byte(debit_account, 120) * 256
local credit_flags = string.byte(credit_account, 119) + string.byte(credit_account, 120) * 256

local ACCOUNT_FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS = 0x0002
local ACCOUNT_FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS = 0x0004

if (math.floor(debit_flags / ACCOUNT_FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS) % 2) == 1 then
    local debit_total = decode_u128(new_debit_account, 33) + decode_u128(new_debit_account, 17)
    local credit_total = decode_u128(new_debit_account, 65) + decode_u128(new_debit_account, 49)
    if debit_total > credit_total then
        return string.char(58) .. string.rep('\0', 127) -- ERR_EXCEEDS_CREDITS = 58
    end
end

if (math.floor(credit_flags / ACCOUNT_FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS) % 2) == 1 then
    local credit_total = decode_u128(new_credit_account, 65) + decode_u128(new_credit_account, 49)
    local debit_total = decode_u128(new_credit_account, 33) + decode_u128(new_credit_account, 17)
    if credit_total > debit_total then
        return string.char(59) .. string.rep('\0', 127) -- ERR_EXCEEDS_DEBITS = 59
    end
end

-- Prepare transfer with timestamp
local transfer_with_ts
local IMPORTED_FLAG = 0x0010  -- For transfers, imported is 0x0010

-- Only set timestamp if imported flag is NOT set
if (math.floor(flags / IMPORTED_FLAG) % 2) == 0 then
    -- imported flag is NOT set, server sets timestamp
    -- TODO: EloqKV doesn't support TIME command in Lua scripts, using arbitrary timestamp
    -- local timestamp = redis.call('TIME')
    -- local ts = tonumber(timestamp[1]) * 1000000000 + tonumber(timestamp[2]) * 1000
    local ts = 1000000000000000000  -- Arbitrary timestamp for EloqKV compatibility

    transfer_with_ts = string.sub(transfer_data, 1, 120) ..
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
else
    -- imported flag IS set, use client-provided timestamp (must be non-zero)
    local ts_bytes = string.sub(transfer_data, 121, 128)
    local ts = 0
    for i = 1, 8 do
        ts = ts + string.byte(ts_bytes, i) * (256 ^ (i - 1))
    end

    -- Timestamp must be non-zero when imported flag is set
    if ts == 0 then
        return string.char(32) .. string.rep('\0', 127) -- ERR_INVALID_DATA_SIZE
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
local debit_has_history = (math.floor(debit_flags / ACCOUNT_FLAG_HISTORY) % 2) == 1
local credit_has_history = (math.floor(credit_flags / ACCOUNT_FLAG_HISTORY) % 2) == 1

-- Helper: encode AccountBalance (64 bytes)
local function encode_account_balance(account_data, transfer_ts)
    -- Extract timestamp from transfer_with_ts
    local ts_bytes = string.sub(transfer_with_ts, 121, 128)

    -- Extract balance fields from account (128 bytes)
    local debits_pending = string.sub(account_data, 17, 32)   -- offset 16, 16 bytes
    local debits_posted = string.sub(account_data, 33, 48)    -- offset 32, 16 bytes
    local credits_pending = string.sub(account_data, 49, 64)  -- offset 48, 16 bytes
    local credits_posted = string.sub(account_data, 65, 80)   -- offset 64, 16 bytes

    -- Return 64-byte AccountBalance: timestamp + balances
    return ts_bytes .. debits_pending .. debits_posted .. credits_pending .. credits_posted
end

if debit_has_history then
    local debit_balance = encode_account_balance(new_debit_account, transfer_with_ts)
    local debit_balance_index = "account:" .. debit_account_id .. ":balance_history"
    redis.call('APPEND', debit_balance_index, debit_balance)
end

if credit_has_history then
    local credit_balance = encode_account_balance(new_credit_account, transfer_with_ts)
    local credit_balance_index = "account:" .. credit_account_id .. ":balance_history"
    redis.call('APPEND', credit_balance_index, credit_balance)
end

-- Return success
return string.char(0) .. string.rep('\0', 127)
