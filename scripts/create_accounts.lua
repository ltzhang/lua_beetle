-- Create Accounts Script
-- Redis Lua script to create accounts with TigerBeetle semantics
-- KEYS: none
-- ARGV: JSON array of account objects

-- Error codes
local ERR_OK = 0
local ERR_ACCOUNT_EXISTS = 1
local ERR_ACCOUNT_INVALID_ID = 3
local ERR_ACCOUNT_INVALID_LEDGER = 4
local ERR_ACCOUNT_INVALID_CODE = 5
local ERR_ACCOUNT_BALANCES_NOT_ZERO = 7
local ERR_LINKED_EVENT_FAILED = 22

-- Account flags
local FLAG_LINKED = 1
local FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS = 2
local FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS = 4
local FLAG_HISTORY = 8
local FLAG_CLOSED = 16

-- Check if a flag is set
local function has_flag(flags, flag)
    return (flags % (flag * 2)) >= flag
end

-- Parse accounts from JSON
local accounts = cjson.decode(ARGV[1])
local results = {}
local all_success = true

-- Process each account
for i, account in ipairs(accounts) do
    local error_code = ERR_OK

    -- Validate ID
    if not account.id or account.id == "" or account.id == "0" then
        error_code = ERR_ACCOUNT_INVALID_ID
    end

    -- Validate ledger
    if error_code == ERR_OK and (not account.ledger or account.ledger == 0) then
        error_code = ERR_ACCOUNT_INVALID_LEDGER
    end

    -- Validate code
    if error_code == ERR_OK and (not account.code or account.code == 0) then
        error_code = ERR_ACCOUNT_INVALID_CODE
    end

    -- Check if account already exists
    if error_code == ERR_OK then
        local key = "account:" .. account.id
        if redis.call('EXISTS', key) == 1 then
            error_code = ERR_ACCOUNT_EXISTS
        end
    end

    -- Validate balances are zero (for new accounts)
    if error_code == ERR_OK then
        local debits_pending = tonumber(account.debits_pending) or 0
        local debits_posted = tonumber(account.debits_posted) or 0
        local credits_pending = tonumber(account.credits_pending) or 0
        local credits_posted = tonumber(account.credits_posted) or 0

        if debits_pending ~= 0 or debits_posted ~= 0 or
           credits_pending ~= 0 or credits_posted ~= 0 then
            error_code = ERR_ACCOUNT_BALANCES_NOT_ZERO
        end
    end

    -- If this account has an error and is linked, fail all
    if error_code ~= ERR_OK then
        all_success = false
        local flags = tonumber(account.flags) or 0

        if has_flag(flags, FLAG_LINKED) then
            -- Roll back all previously created accounts in this batch
            for j = 1, i - 1 do
                local prev_key = "account:" .. accounts[j].id
                redis.call('DEL', prev_key)
            end

            -- Return error for all accounts in linked chain
            results = {}
            for j = 1, #accounts do
                table.insert(results, {
                    index = j - 1,
                    error = j == i and error_code or ERR_LINKED_EVENT_FAILED
                })
            end
            return cjson.encode(results)
        else
            -- Just mark this account as failed
            table.insert(results, {
                index = i - 1,
                error = error_code
            })
        end
    else
        -- Create the account
        local key = "account:" .. account.id
        local flags = tonumber(account.flags) or 0
        local timestamp = redis.call('TIME')
        local ts = tonumber(timestamp[1]) * 1000000000 + tonumber(timestamp[2]) * 1000

        redis.call('HSET', key,
            'id', account.id,
            'debits_pending', '0',
            'debits_posted', '0',
            'credits_pending', '0',
            'credits_posted', '0',
            'user_data_128', account.user_data_128 or '',
            'user_data_64', account.user_data_64 or '',
            'user_data_32', account.user_data_32 or '',
            'ledger', tostring(account.ledger),
            'code', tostring(account.code),
            'flags', tostring(flags),
            'timestamp', tostring(ts)
        )

        -- Success - no error to report
        table.insert(results, {
            index = i - 1,
            error = ERR_OK
        })
    end
end

return cjson.encode(results)
