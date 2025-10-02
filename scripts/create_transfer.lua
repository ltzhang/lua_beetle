-- Create Transfer Script (Single Operation)
-- Redis Lua script to create a single transfer with TigerBeetle semantics
-- KEYS: none
-- ARGV[1]: JSON object representing the transfer

-- Error codes (matching TigerBeetle CreateTransfersResult)
local ERR_OK = 0
local ERR_LINKED_EVENT_FAILED = 1
local ERR_LINKED_EVENT_CHAIN_OPEN = 2
local ERR_ID_MUST_NOT_BE_ZERO = 6
local ERR_FLAGS_ARE_MUTUALLY_EXCLUSIVE = 8
local ERR_DEBIT_ACCOUNT_ID_MUST_NOT_BE_ZERO = 13
local ERR_DEBIT_ACCOUNT_ID_MUST_NOT_BE_INT_MAX = 14
local ERR_CREDIT_ACCOUNT_ID_MUST_NOT_BE_ZERO = 15
local ERR_CREDIT_ACCOUNT_ID_MUST_NOT_BE_INT_MAX = 16
local ERR_ACCOUNTS_MUST_BE_DIFFERENT = 17
local ERR_PENDING_ID_MUST_BE_ZERO = 18
local ERR_PENDING_ID_MUST_NOT_BE_ZERO = 19
local ERR_PENDING_ID_MUST_NOT_BE_INT_MAX = 20
local ERR_PENDING_ID_MUST_BE_DIFFERENT = 21
local ERR_TIMEOUT_RESERVED_FOR_PENDING_TRANSFER = 22
local ERR_LEDGER_MUST_NOT_BE_ZERO = 23
local ERR_CODE_MUST_NOT_BE_ZERO = 24
local ERR_AMOUNT_MUST_NOT_BE_ZERO = 25
local ERR_DEBIT_ACCOUNT_NOT_FOUND = 26
local ERR_CREDIT_ACCOUNT_NOT_FOUND = 27
local ERR_ACCOUNTS_MUST_HAVE_THE_SAME_LEDGER = 28
local ERR_TRANSFER_MUST_HAVE_THE_SAME_LEDGER_AS_ACCOUNTS = 29
local ERR_PENDING_TRANSFER_NOT_FOUND = 30
local ERR_PENDING_TRANSFER_NOT_PENDING = 31
local ERR_PENDING_TRANSFER_HAS_DIFFERENT_DEBIT_ACCOUNT_ID = 32
local ERR_PENDING_TRANSFER_HAS_DIFFERENT_CREDIT_ACCOUNT_ID = 33
local ERR_PENDING_TRANSFER_HAS_DIFFERENT_LEDGER = 34
local ERR_PENDING_TRANSFER_HAS_DIFFERENT_CODE = 35
local ERR_EXCEEDS_PENDING_TRANSFER_AMOUNT = 36
local ERR_PENDING_TRANSFER_HAS_DIFFERENT_AMOUNT = 37
local ERR_PENDING_TRANSFER_ALREADY_POSTED = 38
local ERR_PENDING_TRANSFER_ALREADY_VOIDED = 39
local ERR_PENDING_TRANSFER_EXPIRED = 40
local ERR_EXISTS_WITH_DIFFERENT_FLAGS = 41
local ERR_EXISTS_WITH_DIFFERENT_DEBIT_ACCOUNT_ID = 42
local ERR_EXISTS_WITH_DIFFERENT_CREDIT_ACCOUNT_ID = 43
local ERR_EXISTS_WITH_DIFFERENT_AMOUNT = 44
local ERR_EXISTS_WITH_DIFFERENT_PENDING_ID = 45
local ERR_EXISTS_WITH_DIFFERENT_USER_DATA_128 = 46
local ERR_EXISTS_WITH_DIFFERENT_USER_DATA_64 = 47
local ERR_EXISTS_WITH_DIFFERENT_USER_DATA_32 = 48
local ERR_EXISTS_WITH_DIFFERENT_TIMEOUT = 49
local ERR_EXISTS_WITH_DIFFERENT_CODE = 50
local ERR_EXISTS = 51
local ERR_OVERFLOWS_DEBITS_PENDING = 52
local ERR_OVERFLOWS_CREDITS_PENDING = 53
local ERR_OVERFLOWS_DEBITS_POSTED = 54
local ERR_OVERFLOWS_CREDITS_POSTED = 55
local ERR_OVERFLOWS_DEBITS = 56
local ERR_OVERFLOWS_CREDITS = 57
local ERR_EXCEEDS_CREDITS = 58
local ERR_EXCEEDS_DEBITS = 59

-- Transfer flags
local FLAG_LINKED = 0x0001
local FLAG_PENDING = 0x0002
local FLAG_POST_PENDING_TRANSFER = 0x0004
local FLAG_VOID_PENDING_TRANSFER = 0x0008
local FLAG_BALANCING_DEBIT = 0x0010
local FLAG_BALANCING_CREDIT = 0x0020

-- Account flags
local ACCOUNT_FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS = 0x0002
local ACCOUNT_FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS = 0x0004
local ACCOUNT_FLAG_HISTORY = 0x0008

-- Check if a flag is set
local function has_flag(flags, flag)
    return (flags % (flag * 2)) >= flag
end

-- Parse transfer from JSON
local transfer = cjson.decode(ARGV[1])
local error_code = ERR_OK

-- Validate ID
if not transfer.id or transfer.id == "" or transfer.id == "0" then
    error_code = ERR_ID_MUST_NOT_BE_ZERO
end

-- Validate debit_account_id
if error_code == ERR_OK and (not transfer.debit_account_id or transfer.debit_account_id == "" or transfer.debit_account_id == "0") then
    error_code = ERR_DEBIT_ACCOUNT_ID_MUST_NOT_BE_ZERO
end

-- Validate credit_account_id
if error_code == ERR_OK and (not transfer.credit_account_id or transfer.credit_account_id == "" or transfer.credit_account_id == "0") then
    error_code = ERR_CREDIT_ACCOUNT_ID_MUST_NOT_BE_ZERO
end

-- Validate accounts are different
if error_code == ERR_OK and transfer.debit_account_id == transfer.credit_account_id then
    error_code = ERR_ACCOUNTS_MUST_BE_DIFFERENT
end

-- Validate ledger
if error_code == ERR_OK and (not transfer.ledger or transfer.ledger == 0) then
    error_code = ERR_LEDGER_MUST_NOT_BE_ZERO
end

-- Validate code
if error_code == ERR_OK and (not transfer.code or transfer.code == 0) then
    error_code = ERR_CODE_MUST_NOT_BE_ZERO
end

-- Validate amount
local amount = tonumber(transfer.amount) or 0
if error_code == ERR_OK and amount == 0 then
    error_code = ERR_AMOUNT_MUST_NOT_BE_ZERO
end

-- Parse flags
local flags = tonumber(transfer.flags) or 0

-- Single operations should not have the LINKED flag set
if error_code == ERR_OK and has_flag(flags, FLAG_LINKED) then
    error_code = ERR_LINKED_EVENT_CHAIN_OPEN
end

-- Check if transfer already exists
if error_code == ERR_OK then
    local transfer_key = "transfer:" .. transfer.id
    if redis.call('EXISTS', transfer_key) == 1 then
        error_code = ERR_EXISTS
    end
end

-- Get accounts
local debit_account_key = "account:" .. transfer.debit_account_id
local credit_account_key = "account:" .. transfer.credit_account_id

if error_code == ERR_OK and redis.call('EXISTS', debit_account_key) == 0 then
    error_code = ERR_DEBIT_ACCOUNT_NOT_FOUND
end

if error_code == ERR_OK and redis.call('EXISTS', credit_account_key) == 0 then
    error_code = ERR_CREDIT_ACCOUNT_NOT_FOUND
end

local debit_account = nil
local credit_account = nil

if error_code == ERR_OK then
    debit_account = redis.call('HGETALL', debit_account_key)
    credit_account = redis.call('HGETALL', credit_account_key)

    -- Convert to table
    local debit_tbl = {}
    local credit_tbl = {}
    for i = 1, #debit_account, 2 do
        debit_tbl[debit_account[i]] = debit_account[i + 1]
    end
    for i = 1, #credit_account, 2 do
        credit_tbl[credit_account[i]] = credit_account[i + 1]
    end
    debit_account = debit_tbl
    credit_account = credit_tbl

    -- Validate same ledger
    if debit_account.ledger ~= credit_account.ledger then
        error_code = ERR_ACCOUNTS_MUST_HAVE_THE_SAME_LEDGER
    elseif tostring(transfer.ledger) ~= debit_account.ledger then
        error_code = ERR_TRANSFER_MUST_HAVE_THE_SAME_LEDGER_AS_ACCOUNTS
    end
end

if error_code ~= ERR_OK then
    return cjson.encode({result = error_code})
end

-- Process transfer based on flags
local is_pending = has_flag(flags, FLAG_PENDING)
local is_post = has_flag(flags, FLAG_POST_PENDING_TRANSFER)
local is_void = has_flag(flags, FLAG_VOID_PENDING_TRANSFER)

-- Update account balances
local debit_pending = tonumber(debit_account.debits_pending)
local debit_posted = tonumber(debit_account.debits_posted)
local credit_pending = tonumber(credit_account.credits_pending)
local credit_posted = tonumber(credit_account.credits_posted)

if is_pending then
    debit_pending = debit_pending + amount
    credit_pending = credit_pending + amount
elseif is_post or is_void then
    -- Handle two-phase transfers (simplified for single operation)
    local pending_id = transfer.pending_id or "0"
    if pending_id == "0" then
        error_code = ERR_PENDING_ID_MUST_NOT_BE_ZERO
    end
    -- Additional pending transfer validation would go here
else
    -- Direct posted transfer
    debit_posted = debit_posted + amount
    credit_posted = credit_posted + amount
end

-- Check balance constraints
local debit_flags = tonumber(debit_account.flags)
local credit_flags = tonumber(credit_account.flags)

if has_flag(debit_flags, ACCOUNT_FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS) then
    local debit_total = debit_posted + debit_pending
    local credit_total = tonumber(debit_account.credits_posted) + tonumber(debit_account.credits_pending)
    if debit_total > credit_total then
        error_code = ERR_EXCEEDS_CREDITS
    end
end

if error_code == ERR_OK and has_flag(credit_flags, ACCOUNT_FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS) then
    local credit_total = credit_posted + credit_pending
    local debit_total = tonumber(credit_account.debits_posted) + tonumber(credit_account.debits_pending)
    if credit_total > debit_total then
        error_code = ERR_EXCEEDS_DEBITS
    end
end

if error_code ~= ERR_OK then
    return cjson.encode({result = error_code})
end

-- Update accounts
redis.call('HSET', debit_account_key, 'debits_pending', tostring(debit_pending))
redis.call('HSET', debit_account_key, 'debits_posted', tostring(debit_posted))
redis.call('HSET', credit_account_key, 'credits_pending', tostring(credit_pending))
redis.call('HSET', credit_account_key, 'credits_posted', tostring(credit_posted))

-- Create transfer record
local timestamp = redis.call('TIME')
local ts = tonumber(timestamp[1]) * 1000000000 + tonumber(timestamp[2]) * 1000

local transfer_key = "transfer:" .. transfer.id
redis.call('HSET', transfer_key,
    'id', transfer.id,
    'debit_account_id', transfer.debit_account_id,
    'credit_account_id', transfer.credit_account_id,
    'amount', tostring(amount),
    'pending_id', transfer.pending_id or '0',
    'user_data_128', transfer.user_data_128 or '',
    'user_data_64', transfer.user_data_64 or '',
    'user_data_32', transfer.user_data_32 or '',
    'timeout', transfer.timeout or '0',
    'ledger', tostring(transfer.ledger),
    'code', tostring(transfer.code),
    'flags', tostring(flags),
    'timestamp', tostring(ts)
)

-- Add to secondary indexes
redis.call('ZADD', 'account:' .. transfer.debit_account_id .. ':transfers', ts, transfer.id)
redis.call('ZADD', 'account:' .. transfer.credit_account_id .. ':transfers', ts, transfer.id)

-- Add to balance history if HISTORY flag is set
if has_flag(debit_flags, ACCOUNT_FLAG_HISTORY) then
    local balance_entry = cjson.encode({
        timestamp = ts,
        debits_pending = debit_pending,
        debits_posted = debit_posted,
        credits_pending = tonumber(debit_account.credits_pending),
        credits_posted = tonumber(debit_account.credits_posted)
    })
    redis.call('ZADD', 'account:' .. transfer.debit_account_id .. ':balance_history', ts, balance_entry)
end

if has_flag(credit_flags, ACCOUNT_FLAG_HISTORY) then
    local balance_entry = cjson.encode({
        timestamp = ts,
        debits_pending = tonumber(credit_account.debits_pending),
        debits_posted = tonumber(credit_account.debits_posted),
        credits_pending = credit_pending,
        credits_posted = credit_posted
    })
    redis.call('ZADD', 'account:' .. transfer.credit_account_id .. ':balance_history', ts, balance_entry)
end

return cjson.encode({result = ERR_OK})
