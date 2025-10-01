-- Lua Beetle Utilities
-- Shared functions and constants for TigerBeetle-like operations in Redis
-- Error codes and flags match TigerBeetle specification exactly

-- CreateAccountsResult error codes (matching TigerBeetle enum(u32))
local CreateAccountsResult = {
    ok = 0,
    linked_event_failed = 1,
    linked_event_chain_open = 2,
    timestamp_must_be_zero = 3,
    reserved_field = 4,
    reserved_flag = 5,
    id_must_not_be_zero = 6,
    id_must_not_be_int_max = 7,
    flags_are_mutually_exclusive = 8,
    debits_pending_must_be_zero = 9,
    debits_posted_must_be_zero = 10,
    credits_pending_must_be_zero = 11,
    credits_posted_must_be_zero = 12,
    ledger_must_not_be_zero = 13,
    code_must_not_be_zero = 14,
    exists_with_different_flags = 15,
    exists_with_different_user_data_128 = 16,
    exists_with_different_user_data_64 = 17,
    exists_with_different_user_data_32 = 18,
    exists_with_different_ledger = 19,
    exists_with_different_code = 20,
    exists = 21,
    imported_event_expected = 22,
    imported_event_not_expected = 23,
    imported_event_timestamp_out_of_range = 24,
    imported_event_timestamp_must_not_advance = 25,
    imported_event_timestamp_must_not_regress = 26,
}

-- CreateTransfersResult error codes (matching TigerBeetle enum(u32))
local CreateTransfersResult = {
    ok = 0,
    linked_event_failed = 1,
    linked_event_chain_open = 2,
    timestamp_must_be_zero = 3,
    reserved_flag = 4,
    id_must_not_be_zero = 5,
    id_must_not_be_int_max = 6,
    flags_are_mutually_exclusive = 7,
    debit_account_id_must_not_be_zero = 8,
    debit_account_id_must_not_be_int_max = 9,
    credit_account_id_must_not_be_zero = 10,
    credit_account_id_must_not_be_int_max = 11,
    accounts_must_be_different = 12,
    pending_id_must_be_zero = 13,
    pending_id_must_not_be_zero = 14,
    pending_id_must_not_be_int_max = 15,
    pending_id_must_be_different = 16,
    timeout_reserved_for_pending_transfer = 17,
    deprecated_18 = 18,
    ledger_must_not_be_zero = 19,
    code_must_not_be_zero = 20,
    debit_account_not_found = 21,
    credit_account_not_found = 22,
    accounts_must_have_the_same_ledger = 23,
    transfer_must_have_the_same_ledger_as_accounts = 24,
    pending_transfer_not_found = 25,
    pending_transfer_not_pending = 26,
    pending_transfer_has_different_debit_account_id = 27,
    pending_transfer_has_different_credit_account_id = 28,
    pending_transfer_has_different_ledger = 29,
    pending_transfer_has_different_code = 30,
    exceeds_pending_transfer_amount = 31,
    pending_transfer_has_different_amount = 32,
    pending_transfer_already_posted = 33,
    pending_transfer_already_voided = 34,
    pending_transfer_expired = 35,
    exists_with_different_flags = 36,
    exists_with_different_debit_account_id = 37,
    exists_with_different_credit_account_id = 38,
    exists_with_different_amount = 39,
    exists_with_different_pending_id = 40,
    exists_with_different_user_data_128 = 41,
    exists_with_different_user_data_64 = 42,
    exists_with_different_user_data_32 = 43,
    exists_with_different_timeout = 44,
    exists_with_different_code = 45,
    exists = 46,
    overflows_debits_pending = 47,
    overflows_credits_pending = 48,
    overflows_debits_posted = 49,
    overflows_credits_posted = 50,
    overflows_debits = 51,
    overflows_credits = 52,
    overflows_timeout = 53,
    exceeds_credits = 54,
    exceeds_debits = 55,
}

-- Account flags (matching TigerBeetle packed struct(u16))
local AccountFlags = {
    none = 0,
    linked = 0x0001,                                    -- 1 << 0
    debits_must_not_exceed_credits = 0x0002,           -- 1 << 1
    credits_must_not_exceed_debits = 0x0004,           -- 1 << 2
    history = 0x0008,                                   -- 1 << 3
    imported = 0x0010,                                  -- 1 << 4
    closed = 0x0020,                                    -- 1 << 5
}

-- Transfer flags (matching TigerBeetle packed struct(u16))
local TransferFlags = {
    none = 0,
    linked = 0x0001,                    -- 1 << 0
    pending = 0x0002,                   -- 1 << 1
    post_pending_transfer = 0x0004,     -- 1 << 2
    void_pending_transfer = 0x0008,     -- 1 << 3
    balancing_debit = 0x0010,           -- 1 << 4
    balancing_credit = 0x0020,          -- 1 << 5
    closing_debit = 0x0040,             -- 1 << 6
    closing_credit = 0x0080,            -- 1 << 7
    imported = 0x0100,                  -- 1 << 8
}

-- Check if a flag is set
local function has_flag(flags, flag)
    return (flags % (flag * 2)) >= flag
end

-- Parse integer with default
local function parse_int(value, default)
    if not value or value == "" then
        return default or 0
    end
    return tonumber(value) or (default or 0)
end

-- Get current timestamp in nanoseconds (simulated)
local function get_timestamp()
    local time = redis.call('TIME')
    -- TIME returns {seconds, microseconds}
    return tonumber(time[1]) * 1000000000 + tonumber(time[2]) * 1000
end

-- Export functions and constants
return {
    CreateAccountsResult = CreateAccountsResult,
    CreateTransfersResult = CreateTransfersResult,
    AccountFlags = AccountFlags,
    TransferFlags = TransferFlags,
    has_flag = has_flag,
    parse_int = parse_int,
    get_timestamp = get_timestamp,
}
