-- Create Account Script (Binary Encoding)
-- Binary layout matches TigerBeetle format: 128 bytes fixed size
-- KEYS: none
-- ARGV[1]: account binary data (128 bytes)

-- Binary layout (128 bytes total):
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

local account_data = ARGV[1]

local ERR_INVALID_DATA_SIZE = 32
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

local FLAG_LINKED = 0x0001
local FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS = 0x0002
local FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS = 0x0004
local FLAG_IMPORTED = 0x0010
local ACCOUNT_FLAGS_MASK = 0x003f

-- Validate size
if #account_data ~= 128 then
    return lb_result(ERR_INVALID_DATA_SIZE)
end

-- Extract id (first 16 bytes) for key
local id = string.sub(account_data, 1, 16)
local key = "account:" .. id

-- Extract flags (2 bytes at offset 118)
local flags_byte1 = string.byte(account_data, 119)
local flags_byte2 = string.byte(account_data, 120)
local flags = flags_byte1 + flags_byte2 * 256

-- Check for LINKED flag (0x0001)
if lb_has_flag(flags, FLAG_LINKED) then
    return lb_result(ERR_LINKED_EVENT_CHAIN_OPEN)
end

if flags > ACCOUNT_FLAGS_MASK then
    return lb_result(ERR_RESERVED_FLAG)
end

if id == lb_zero_16 then
    return lb_result(ERR_ID_MUST_NOT_BE_ZERO)
end

if id == string.rep('\255', 16) then
    return lb_result(ERR_ID_MUST_NOT_BE_INT_MAX)
end

if not lb_all_zero(account_data, 17, 16) then
    return lb_result(ERR_DEBITS_PENDING_MUST_BE_ZERO)
end

if not lb_all_zero(account_data, 33, 16) then
    return lb_result(ERR_DEBITS_POSTED_MUST_BE_ZERO)
end

if not lb_all_zero(account_data, 49, 16) then
    return lb_result(ERR_CREDITS_PENDING_MUST_BE_ZERO)
end

if not lb_all_zero(account_data, 65, 16) then
    return lb_result(ERR_CREDITS_POSTED_MUST_BE_ZERO)
end

if not lb_all_zero(account_data, 109, 4) then
    return lb_result(ERR_RESERVED_FIELD)
end

if lb_decode_u32(account_data, 113) == 0 then
    return lb_result(ERR_LEDGER_MUST_NOT_BE_ZERO)
end

if lb_decode_u16(account_data, 117) == 0 then
    return lb_result(ERR_CODE_MUST_NOT_BE_ZERO)
end

if lb_has_flag(flags, FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS) and
   lb_has_flag(flags, FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS) then
    return lb_result(ERR_FLAGS_ARE_MUTUALLY_EXCLUSIVE)
end

local client_timestamp = lb_decode_u64(account_data, 121)
if lb_has_flag(flags, FLAG_IMPORTED) then
    -- Imported-event ordering semantics are not implemented in lua_beetle.
    return lb_result(ERR_RESERVED_FLAG)
else
    if client_timestamp ~= 0 then
        return lb_result(ERR_TIMESTAMP_MUST_BE_ZERO)
    end
end

-- Check if account already exists
local existing = redis.call('GET', key)
if existing and #existing == 128 then
    local existing_flags = lb_decode_u16(existing, 119)
    if existing_flags ~= flags then
        return lb_result(ERR_EXISTS_WITH_DIFFERENT_FLAGS)
    end

    if string.sub(existing, 81, 96) ~= string.sub(account_data, 81, 96) then
        return lb_result(ERR_EXISTS_WITH_DIFFERENT_USER_DATA_128)
    end

    if lb_decode_u64(existing, 97) ~= lb_decode_u64(account_data, 97) then
        return lb_result(ERR_EXISTS_WITH_DIFFERENT_USER_DATA_64)
    end

    if lb_decode_u32(existing, 105) ~= lb_decode_u32(account_data, 105) then
        return lb_result(ERR_EXISTS_WITH_DIFFERENT_USER_DATA_32)
    end

    if lb_decode_u32(existing, 113) ~= lb_decode_u32(account_data, 113) then
        return lb_result(ERR_EXISTS_WITH_DIFFERENT_LEDGER)
    end

    if lb_decode_u16(existing, 117) ~= lb_decode_u16(account_data, 117) then
        return lb_result(ERR_EXISTS_WITH_DIFFERENT_CODE)
    end

    return lb_result(ERR_EXISTS)
end

-- Prepare account data with timestamp
local account_with_ts
local time = redis.call('TIME')
local DEFAULT_TIMESTAMP = lb_encode_u64(tonumber(time[1]) * 1000000000 + tonumber(time[2]) * 1000)

-- Only set timestamp if imported flag is NOT set
if not lb_has_flag(flags, FLAG_IMPORTED) then
    -- imported flag is NOT set, server sets timestamp
    account_with_ts = string.sub(account_data, 1, 120) .. DEFAULT_TIMESTAMP
else
    account_with_ts = account_data
end

-- Store as single binary string
redis.call('SET', key, account_with_ts)

-- Return success (ERR_OK = 0)
return lb_result(0)
