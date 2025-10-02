-- Lookup Account Script (Binary Encoding)
-- Returns the 128-byte binary account data
-- KEYS: none
-- ARGV[1]: account ID (16 bytes binary)

local account_id = ARGV[1]

-- Validate ID size
if #account_id ~= 16 then
    return "" -- Empty response for invalid ID
end

local key = "account:" .. account_id

-- Get account data
local account = redis.call('GET', key)

if not account or #account ~= 128 then
    return "" -- Return empty for non-existent account
end

-- Return the raw 128-byte binary data
return account
