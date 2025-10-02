-- Create Accounts Script
-- Redis Lua script to create accounts with TigerBeetle semantics
-- KEYS: none
-- ARGV: JSON array of account objects

-- Error codes (matching TigerBeetle CreateAccountsResult)
local ERR_OK = 0
local ERR_LINKED_EVENT_FAILED = 1
local ERR_LINKED_EVENT_CHAIN_OPEN = 2
local ERR_ID_MUST_NOT_BE_ZERO = 6
local ERR_FLAGS_ARE_MUTUALLY_EXCLUSIVE = 8
local ERR_DEBITS_PENDING_MUST_BE_ZERO = 9
local ERR_DEBITS_POSTED_MUST_BE_ZERO = 10
local ERR_CREDITS_PENDING_MUST_BE_ZERO = 11
local ERR_CREDITS_POSTED_MUST_BE_ZERO = 12
local ERR_LEDGER_MUST_NOT_BE_ZERO = 13
local ERR_CODE_MUST_NOT_BE_ZERO = 14
local ERR_EXISTS = 21

-- Account flags (matching TigerBeetle)
local FLAG_LINKED = 0x0001                                  -- 1 << 0
local FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS = 0x0002         -- 1 << 1
local FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS = 0x0004         -- 1 << 2
local FLAG_HISTORY = 0x0008                                 -- 1 << 3
local FLAG_IMPORTED = 0x0010                                -- 1 << 4
local FLAG_CLOSED = 0x0020                                  -- 1 << 5

-- Check if a flag is set
local function has_flag(flags, flag)
    return (flags % (flag * 2)) >= flag
end

-- Parse accounts from JSON
local accounts = cjson.decode(ARGV[1])
local results = {}
local chain_start = nil  -- Start index of current linked chain

-- Process each account
for i, account in ipairs(accounts) do
    local error_code = ERR_OK

    -- Validate ID (id_must_not_be_zero)
    if not account.id or account.id == "" or account.id == "0" then
        error_code = ERR_ID_MUST_NOT_BE_ZERO
    end

    -- Validate ledger (ledger_must_not_be_zero)
    if error_code == ERR_OK and (not account.ledger or account.ledger == 0) then
        error_code = ERR_LEDGER_MUST_NOT_BE_ZERO
    end

    -- Validate code (code_must_not_be_zero)
    if error_code == ERR_OK and (not account.code or account.code == 0) then
        error_code = ERR_CODE_MUST_NOT_BE_ZERO
    end

    -- Parse flags
    local flags = tonumber(account.flags) or 0

    -- Check for mutually exclusive flags (flags_are_mutually_exclusive)
    if error_code == ERR_OK then
        local has_debits_constraint = has_flag(flags, FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS)
        local has_credits_constraint = has_flag(flags, FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS)
        if has_debits_constraint and has_credits_constraint then
            error_code = ERR_FLAGS_ARE_MUTUALLY_EXCLUSIVE
        end
    end

    -- Check if account already exists
    if error_code == ERR_OK then
        local key = "account:" .. account.id
        if redis.call('EXISTS', key) == 1 then
            error_code = ERR_EXISTS
        end
    end

    -- Validate balances are zero (for new accounts)
    if error_code == ERR_OK then
        local debits_pending = tonumber(account.debits_pending) or 0
        local debits_posted = tonumber(account.debits_posted) or 0
        local credits_pending = tonumber(account.credits_pending) or 0
        local credits_posted = tonumber(account.credits_posted) or 0

        if debits_pending ~= 0 then
            error_code = ERR_DEBITS_PENDING_MUST_BE_ZERO
        elseif debits_posted ~= 0 then
            error_code = ERR_DEBITS_POSTED_MUST_BE_ZERO
        elseif credits_pending ~= 0 then
            error_code = ERR_CREDITS_PENDING_MUST_BE_ZERO
        elseif credits_posted ~= 0 then
            error_code = ERR_CREDITS_POSTED_MUST_BE_ZERO
        end
    end

    -- Track linked chains
    local is_linked = has_flag(flags, FLAG_LINKED)
    if chain_start == nil and is_linked then
        -- Start of a new linked chain
        chain_start = i
    end

    -- Handle errors and success
    if error_code ~= ERR_OK then
        if chain_start ~= nil then
            -- We're in a linked chain - roll back the entire chain
            for j = chain_start, i - 1 do
                local prev_account = accounts[j]
                local prev_key = "account:" .. prev_account.id
                redis.call('DEL', prev_key)
            end

            -- Mark all accounts in the chain as failed
            for j = chain_start, i do
                table.insert(results, {
                    index = j - 1,
                    result = j == i and error_code or ERR_LINKED_EVENT_FAILED
                })
            end

            -- Reset chain
            chain_start = nil
        else
            -- Not in a chain, just fail this account
            table.insert(results, {
                index = i - 1,
                result = error_code
            })
        end
    elseif error_code == ERR_OK then
        -- Success - create the account
        local key = "account:" .. account.id
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

        -- If this account is NOT linked, report success and end chain
        -- If it IS linked, don't report yet (wait for chain to complete)
        if not is_linked then
            -- Chain ending or standalone account - report success for all
            if chain_start ~= nil then
                -- Ending a chain - report all accounts in chain
                for j = chain_start, i do
                    table.insert(results, {
                        index = j - 1,
                        result = ERR_OK
                    })
                end
                chain_start = nil
            else
                -- Standalone account - just report this one
                table.insert(results, {
                    index = i - 1,
                    result = ERR_OK
                })
            end
        end
        -- If linked, don't report yet - continue the chain
    end
end

-- Check if there's an open linked chain at the end (error: linked_event_chain_open)
if chain_start ~= nil then
    -- Roll back the entire unclosed chain
    for j = chain_start, #accounts do
        local prev_account = accounts[j]
        local prev_key = "account:" .. prev_account.id
        redis.call('DEL', prev_key)
        -- Update result for this account
        results[j] = {
            index = j - 1,
            result = ERR_LINKED_EVENT_CHAIN_OPEN
        }
    end
end

return cjson.encode(results)
