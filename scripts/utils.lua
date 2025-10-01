-- Lua Beetle Utilities
-- Shared functions and constants for TigerBeetle-like operations in Redis

-- Error codes matching TigerBeetle semantics
local ErrorCodes = {
    OK = 0,
    -- Account errors
    ACCOUNT_EXISTS = 1,
    ACCOUNT_NOT_FOUND = 2,
    ACCOUNT_INVALID_ID = 3,
    ACCOUNT_INVALID_LEDGER = 4,
    ACCOUNT_INVALID_CODE = 5,
    ACCOUNT_INVALID_FLAGS = 6,
    ACCOUNT_BALANCES_NOT_ZERO = 7,
    ACCOUNT_CLOSED = 8,
    -- Transfer errors
    TRANSFER_EXISTS = 9,
    TRANSFER_INVALID_ID = 10,
    TRANSFER_INVALID_DEBIT_ACCOUNT = 11,
    TRANSFER_INVALID_CREDIT_ACCOUNT = 12,
    TRANSFER_ACCOUNTS_SAME = 13,
    TRANSFER_INVALID_AMOUNT = 14,
    TRANSFER_INVALID_LEDGER = 15,
    TRANSFER_INVALID_CODE = 16,
    TRANSFER_LEDGER_MISMATCH = 17,
    TRANSFER_EXCEEDS_CREDITS = 18,
    TRANSFER_EXCEEDS_DEBITS = 19,
    TRANSFER_PENDING_NOT_FOUND = 20,
    TRANSFER_PENDING_EXPIRED = 21,
    LINKED_EVENT_FAILED = 22,
}

-- Account flags
local AccountFlags = {
    NONE = 0,
    LINKED = 1,                                    -- 1 << 0
    DEBITS_MUST_NOT_EXCEED_CREDITS = 2,           -- 1 << 1
    CREDITS_MUST_NOT_EXCEED_DEBITS = 4,           -- 1 << 2
    HISTORY = 8,                                   -- 1 << 3
    CLOSED = 16,                                   -- 1 << 4
}

-- Transfer flags
local TransferFlags = {
    NONE = 0,
    LINKED = 1,                    -- 1 << 0
    PENDING = 2,                   -- 1 << 1
    POST_PENDING_TRANSFER = 4,     -- 1 << 2
    VOID_PENDING_TRANSFER = 8,     -- 1 << 3
    BALANCING_DEBIT = 16,          -- 1 << 4
    BALANCING_CREDIT = 32,         -- 1 << 5
    CLOSING_DEBIT = 64,            -- 1 << 6
    CLOSING_CREDIT = 128,          -- 1 << 7
}

-- Check if a flag is set
local function has_flag(flags, flag)
    return (flags % (flag * 2)) >= flag
end

-- Validate account ID
local function validate_account_id(id)
    if not id or id == "" or id == "0" then
        return false, ErrorCodes.ACCOUNT_INVALID_ID
    end
    return true, ErrorCodes.OK
end

-- Validate transfer ID
local function validate_transfer_id(id)
    if not id or id == "" or id == "0" then
        return false, ErrorCodes.TRANSFER_INVALID_ID
    end
    return true, ErrorCodes.OK
end

-- Parse integer with default
local function parse_int(value, default)
    if not value or value == "" then
        return default or 0
    end
    return tonumber(value) or (default or 0)
end

-- Serialize error result
local function error_result(index, error_code)
    return cjson.encode({
        index = index,
        error = error_code
    })
end

-- Serialize success result
local function success_result()
    return cjson.encode({
        error = ErrorCodes.OK
    })
end

-- Get current timestamp in nanoseconds (simulated)
local function get_timestamp()
    local time = redis.call('TIME')
    -- TIME returns {seconds, microseconds}
    return tonumber(time[1]) * 1000000000 + tonumber(time[2]) * 1000
end

-- Export functions and constants
return {
    ErrorCodes = ErrorCodes,
    AccountFlags = AccountFlags,
    TransferFlags = TransferFlags,
    has_flag = has_flag,
    validate_account_id = validate_account_id,
    validate_transfer_id = validate_transfer_id,
    parse_int = parse_int,
    error_result = error_result,
    success_result = success_result,
    get_timestamp = get_timestamp,
}
