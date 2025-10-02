-- Get Account Transfers Script (Binary Encoding)
-- Returns concatenated 128-byte binary transfer data
-- KEYS: none
-- ARGV[1]: account_id (16 bytes binary)
-- ARGV[2]: (optional) limit (default: -1 for all)

local account_id = ARGV[1]
local limit = tonumber(ARGV[2]) or -1

-- Validate ID size
if #account_id ~= 16 then
    return "" -- Empty response for invalid ID
end

-- Get transfer index (simple string of concatenated 16-byte IDs)
local index_key = "account:" .. account_id .. ":transfers"
local index_data = redis.call('GET', index_key)

if not index_data then
    return "" -- No transfers for this account
end

-- Each transfer ID is 16 bytes
local num_transfers = math.floor(#index_data / 16)
local actual_limit = limit
if limit == -1 or limit > num_transfers then
    actual_limit = num_transfers
end

-- Build result by fetching each transfer
local results = {}
for i = 1, actual_limit do
    local offset = (i - 1) * 16 + 1
    local transfer_id = string.sub(index_data, offset, offset + 15)
    local transfer_key = "transfer:" .. transfer_id

    local transfer = redis.call('GET', transfer_key)
    if transfer and #transfer == 128 then
        table.insert(results, transfer)
    end
end

-- Return concatenated binary transfers
return table.concat(results)
