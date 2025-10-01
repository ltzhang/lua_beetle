-- Create Transfers Script
-- Redis Lua script to create transfers with TigerBeetle semantics
-- KEYS: none
-- ARGV: JSON array of transfer objects

-- Error codes
local ERR_OK = 0
local ERR_TRANSFER_EXISTS = 9
local ERR_TRANSFER_INVALID_ID = 10
local ERR_TRANSFER_INVALID_DEBIT_ACCOUNT = 11
local ERR_TRANSFER_INVALID_CREDIT_ACCOUNT = 12
local ERR_TRANSFER_ACCOUNTS_SAME = 13
local ERR_TRANSFER_INVALID_AMOUNT = 14
local ERR_TRANSFER_INVALID_LEDGER = 15
local ERR_TRANSFER_LEDGER_MISMATCH = 17
local ERR_TRANSFER_EXCEEDS_CREDITS = 18
local ERR_TRANSFER_EXCEEDS_DEBITS = 19
local ERR_TRANSFER_PENDING_NOT_FOUND = 20
local ERR_LINKED_EVENT_FAILED = 22
local ERR_ACCOUNT_CLOSED = 8

-- Transfer flags
local FLAG_LINKED = 1
local FLAG_PENDING = 2
local FLAG_POST_PENDING_TRANSFER = 4
local FLAG_VOID_PENDING_TRANSFER = 8
local FLAG_BALANCING_DEBIT = 16
local FLAG_BALANCING_CREDIT = 32
local FLAG_CLOSING_DEBIT = 64
local FLAG_CLOSING_CREDIT = 128

-- Account flags
local ACCOUNT_FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS = 2
local ACCOUNT_FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS = 4
local ACCOUNT_FLAG_HISTORY = 8
local ACCOUNT_FLAG_CLOSED = 16

-- Check if a flag is set
local function has_flag(flags, flag)
    return (flags % (flag * 2)) >= flag
end

-- Parse transfers from JSON
local transfers = cjson.decode(ARGV[1])
local results = {}

-- Process each transfer
for i, transfer in ipairs(transfers) do
    local error_code = ERR_OK

    -- Validate ID
    if not transfer.id or transfer.id == "" or transfer.id == "0" then
        error_code = ERR_TRANSFER_INVALID_ID
    end

    -- Validate ledger
    if error_code == ERR_OK and (not transfer.ledger or transfer.ledger == 0) then
        error_code = ERR_TRANSFER_INVALID_LEDGER
    end

    -- Check if transfer already exists
    if error_code == ERR_OK then
        local key = "transfer:" .. transfer.id
        if redis.call('EXISTS', key) == 1 then
            error_code = ERR_TRANSFER_EXISTS
        end
    end

    -- Validate and load debit account
    local debit_account_key = nil
    local debit_account = nil
    if error_code == ERR_OK then
        if not transfer.debit_account_id or transfer.debit_account_id == "" then
            error_code = ERR_TRANSFER_INVALID_DEBIT_ACCOUNT
        else
            debit_account_key = "account:" .. transfer.debit_account_id
            if redis.call('EXISTS', debit_account_key) == 0 then
                error_code = ERR_TRANSFER_INVALID_DEBIT_ACCOUNT
            else
                debit_account = redis.call('HGETALL', debit_account_key)
                -- Convert array to map
                local temp = {}
                for j = 1, #debit_account, 2 do
                    temp[debit_account[j]] = debit_account[j + 1]
                end
                debit_account = temp
            end
        end
    end

    -- Validate and load credit account
    local credit_account_key = nil
    local credit_account = nil
    if error_code == ERR_OK then
        if not transfer.credit_account_id or transfer.credit_account_id == "" then
            error_code = ERR_TRANSFER_INVALID_CREDIT_ACCOUNT
        else
            credit_account_key = "account:" .. transfer.credit_account_id
            if redis.call('EXISTS', credit_account_key) == 0 then
                error_code = ERR_TRANSFER_INVALID_CREDIT_ACCOUNT
            else
                credit_account = redis.call('HGETALL', credit_account_key)
                -- Convert array to map
                local temp = {}
                for j = 1, #credit_account, 2 do
                    temp[credit_account[j]] = credit_account[j + 1]
                end
                credit_account = temp
            end
        end
    end

    -- Check accounts are not the same
    if error_code == ERR_OK then
        if transfer.debit_account_id == transfer.credit_account_id then
            error_code = ERR_TRANSFER_ACCOUNTS_SAME
        end
    end

    -- Validate amount
    local amount = tonumber(transfer.amount) or 0
    if error_code == ERR_OK and amount <= 0 then
        error_code = ERR_TRANSFER_INVALID_AMOUNT
    end

    -- Check ledgers match
    if error_code == ERR_OK then
        local debit_ledger = tonumber(debit_account.ledger)
        local credit_ledger = tonumber(credit_account.ledger)
        if debit_ledger ~= transfer.ledger or credit_ledger ~= transfer.ledger then
            error_code = ERR_TRANSFER_LEDGER_MISMATCH
        end
    end

    -- Check accounts are not closed
    if error_code == ERR_OK then
        local debit_flags = tonumber(debit_account.flags) or 0
        local credit_flags = tonumber(credit_account.flags) or 0
        if has_flag(debit_flags, ACCOUNT_FLAG_CLOSED) or has_flag(credit_flags, ACCOUNT_FLAG_CLOSED) then
            error_code = ERR_ACCOUNT_CLOSED
        end
    end

    local flags = tonumber(transfer.flags) or 0
    local is_pending = has_flag(flags, FLAG_PENDING)
    local is_post = has_flag(flags, FLAG_POST_PENDING_TRANSFER)
    local is_void = has_flag(flags, FLAG_VOID_PENDING_TRANSFER)

    -- Handle pending transfer operations
    if error_code == ERR_OK and (is_post or is_void) then
        -- This is a post or void operation
        if not transfer.pending_id or transfer.pending_id == "" then
            error_code = ERR_TRANSFER_PENDING_NOT_FOUND
        else
            local pending_key = "transfer:" .. transfer.pending_id
            if redis.call('EXISTS', pending_key) == 0 then
                error_code = ERR_TRANSFER_PENDING_NOT_FOUND
            else
                local pending_transfer = redis.call('HGETALL', pending_key)
                local temp = {}
                for j = 1, #pending_transfer, 2 do
                    temp[pending_transfer[j]] = pending_transfer[j + 1]
                end
                pending_transfer = temp

                -- Check if it's actually pending
                local pending_flags = tonumber(pending_transfer.flags) or 0
                if not has_flag(pending_flags, FLAG_PENDING) then
                    error_code = ERR_TRANSFER_PENDING_NOT_FOUND
                else
                    -- Override transfer amount and accounts from pending transfer
                    amount = tonumber(pending_transfer.amount)
                    transfer.debit_account_id = pending_transfer.debit_account_id
                    transfer.credit_account_id = pending_transfer.credit_account_id
                    debit_account_key = "account:" .. transfer.debit_account_id
                    credit_account_key = "account:" .. transfer.credit_account_id

                    -- Reload accounts
                    debit_account = redis.call('HGETALL', debit_account_key)
                    temp = {}
                    for j = 1, #debit_account, 2 do
                        temp[debit_account[j]] = debit_account[j + 1]
                    end
                    debit_account = temp

                    credit_account = redis.call('HGETALL', credit_account_key)
                    temp = {}
                    for j = 1, #credit_account, 2 do
                        temp[credit_account[j]] = credit_account[j + 1]
                    end
                    credit_account = temp
                end
            end
        end
    end

    -- Apply the transfer
    if error_code == ERR_OK then
        local debit_pending = tonumber(debit_account.debits_pending) or 0
        local debit_posted = tonumber(debit_account.debits_posted) or 0
        local credit_pending = tonumber(credit_account.credits_pending) or 0
        local credit_posted = tonumber(credit_account.credits_posted) or 0

        local debit_flags = tonumber(debit_account.flags) or 0
        local credit_flags = tonumber(credit_account.flags) or 0

        if is_pending then
            -- Add to pending balances
            debit_pending = debit_pending + amount
            credit_pending = credit_pending + amount
        elseif is_post then
            -- Move from pending to posted
            debit_pending = debit_pending - amount
            debit_posted = debit_posted + amount
            credit_pending = credit_pending - amount
            credit_posted = credit_posted + amount
        elseif is_void then
            -- Remove from pending
            debit_pending = debit_pending - amount
            credit_pending = credit_pending - amount
        else
            -- Single-phase: directly posted
            debit_posted = debit_posted + amount
            credit_posted = credit_posted + amount
        end

        -- Check balance constraints for debit account
        if has_flag(debit_flags, ACCOUNT_FLAG_DEBITS_MUST_NOT_EXCEED_CREDITS) then
            local debit_total = debit_posted + debit_pending
            local credit_total = tonumber(debit_account.credits_posted) + tonumber(debit_account.credits_pending)
            if debit_total > credit_total then
                error_code = ERR_TRANSFER_EXCEEDS_CREDITS
            end
        end

        -- Check balance constraints for credit account
        if error_code == ERR_OK and has_flag(credit_flags, ACCOUNT_FLAG_CREDITS_MUST_NOT_EXCEED_DEBITS) then
            local credit_total = credit_posted + credit_pending
            local debit_total = tonumber(credit_account.debits_posted) + tonumber(credit_account.debits_pending)
            if credit_total > debit_total then
                error_code = ERR_TRANSFER_EXCEEDS_DEBITS
            end
        end

        -- Update accounts if no constraint violations
        if error_code == ERR_OK then
            redis.call('HSET', debit_account_key, 'debits_pending', tostring(debit_pending))
            redis.call('HSET', debit_account_key, 'debits_posted', tostring(debit_posted))
            redis.call('HSET', credit_account_key, 'credits_pending', tostring(credit_pending))
            redis.call('HSET', credit_account_key, 'credits_posted', tostring(credit_posted))

            -- Create transfer record
            local timestamp = redis.call('TIME')
            local ts = tonumber(timestamp[1]) * 1000000000 + tonumber(timestamp[2]) * 1000
            local key = "transfer:" .. transfer.id

            redis.call('HSET', key,
                'id', transfer.id,
                'debit_account_id', transfer.debit_account_id,
                'credit_account_id', transfer.credit_account_id,
                'amount', tostring(amount),
                'pending_id', transfer.pending_id or '',
                'user_data_128', transfer.user_data_128 or '',
                'user_data_64', transfer.user_data_64 or '',
                'user_data_32', transfer.user_data_32 or '',
                'timeout', transfer.timeout or 0,
                'ledger', tostring(transfer.ledger),
                'code', tostring(transfer.code or 0),
                'flags', tostring(flags),
                'timestamp', tostring(ts)
            )

            -- Add transfer to account indexes (for get_account_transfers)
            redis.call('ZADD',
                'account:' .. transfer.debit_account_id .. ':transfers',
                ts,
                transfer.id)
            redis.call('ZADD',
                'account:' .. transfer.credit_account_id .. ':transfers',
                ts,
                transfer.id)

            -- Add balance history if HISTORY flag is set on accounts
            if has_flag(debit_flags, ACCOUNT_FLAG_HISTORY) then
                local balance_entry = cjson.encode({
                    timestamp = ts,
                    debits_pending = debit_pending,
                    debits_posted = debit_posted,
                    credits_pending = tonumber(debit_account.credits_pending),
                    credits_posted = tonumber(debit_account.credits_posted),
                    transfer_id = transfer.id
                })
                redis.call('ZADD',
                    'account:' .. transfer.debit_account_id .. ':balance_history',
                    ts,
                    balance_entry)
            end

            if has_flag(credit_flags, ACCOUNT_FLAG_HISTORY) then
                local balance_entry = cjson.encode({
                    timestamp = ts,
                    debits_pending = tonumber(credit_account.debits_pending),
                    debits_posted = tonumber(credit_account.debits_posted),
                    credits_pending = credit_pending,
                    credits_posted = credit_posted,
                    transfer_id = transfer.id
                })
                redis.call('ZADD',
                    'account:' .. transfer.credit_account_id .. ':balance_history',
                    ts,
                    balance_entry)
            end
        end
    end

    -- Handle errors and linked flag
    if error_code ~= ERR_OK then
        if has_flag(flags, FLAG_LINKED) then
            -- Roll back all previously created transfers in this batch
            for j = 1, i - 1 do
                local prev_transfer = transfers[j]
                local prev_key = "transfer:" .. prev_transfer.id

                -- Get the transfer to rollback
                if redis.call('EXISTS', prev_key) == 1 then
                    local prev_data = redis.call('HGETALL', prev_key)
                    local temp = {}
                    for k = 1, #prev_data, 2 do
                        temp[prev_data[k]] = prev_data[k + 1]
                    end
                    prev_data = temp

                    -- Reverse the account updates
                    local prev_amount = tonumber(prev_data.amount)
                    local prev_flags = tonumber(prev_data.flags)
                    local prev_debit_key = "account:" .. prev_data.debit_account_id
                    local prev_credit_key = "account:" .. prev_data.credit_account_id

                    local was_pending = has_flag(prev_flags, FLAG_PENDING)
                    local was_post = has_flag(prev_flags, FLAG_POST_PENDING_TRANSFER)
                    local was_void = has_flag(prev_flags, FLAG_VOID_PENDING_TRANSFER)

                    if was_pending then
                        redis.call('HINCRBY', prev_debit_key, 'debits_pending', -prev_amount)
                        redis.call('HINCRBY', prev_credit_key, 'credits_pending', -prev_amount)
                    elseif was_post then
                        redis.call('HINCRBY', prev_debit_key, 'debits_pending', prev_amount)
                        redis.call('HINCRBY', prev_debit_key, 'debits_posted', -prev_amount)
                        redis.call('HINCRBY', prev_credit_key, 'credits_pending', prev_amount)
                        redis.call('HINCRBY', prev_credit_key, 'credits_posted', -prev_amount)
                    elseif was_void then
                        redis.call('HINCRBY', prev_debit_key, 'debits_pending', prev_amount)
                        redis.call('HINCRBY', prev_credit_key, 'credits_pending', prev_amount)
                    else
                        redis.call('HINCRBY', prev_debit_key, 'debits_posted', -prev_amount)
                        redis.call('HINCRBY', prev_credit_key, 'credits_posted', -prev_amount)
                    end

                    -- Delete the transfer
                    redis.call('DEL', prev_key)

                    -- Remove from transfer indexes
                    redis.call('ZREM', 'account:' .. prev_data.debit_account_id .. ':transfers', prev_data.id)
                    redis.call('ZREM', 'account:' .. prev_data.credit_account_id .. ':transfers', prev_data.id)

                    -- Remove from balance history (if exists)
                    local prev_ts = prev_data.timestamp
                    redis.call('ZREMRANGEBYSCORE',
                        'account:' .. prev_data.debit_account_id .. ':balance_history',
                        prev_ts, prev_ts)
                    redis.call('ZREMRANGEBYSCORE',
                        'account:' .. prev_data.credit_account_id .. ':balance_history',
                        prev_ts, prev_ts)
                end
            end

            -- Return error for all transfers in linked chain
            results = {}
            for j = 1, #transfers do
                table.insert(results, {
                    index = j - 1,
                    error = j == i and error_code or ERR_LINKED_EVENT_FAILED
                })
            end
            return cjson.encode(results)
        else
            -- Just mark this transfer as failed
            table.insert(results, {
                index = i - 1,
                error = error_code
            })
        end
    else
        -- Success
        table.insert(results, {
            index = i - 1,
            error = ERR_OK
        })
    end
end

return cjson.encode(results)
