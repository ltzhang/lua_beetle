-- Lookup Transfer Script (Binary Encoding)
-- Returns the 128-byte binary transfer data
-- KEYS: none
-- ARGV[1]: transfer ID (16 bytes binary)

local transfer_id = ARGV[1]

-- Validate ID size
if #transfer_id ~= 16 then
    return "" -- Empty response for invalid ID
end

local key = "transfer:" .. transfer_id

-- Get transfer data
local transfer = redis.call('GET', key)

if not transfer or #transfer ~= 128 then
    return "" -- Return empty for non-existent transfer
end

-- Return the raw 128-byte binary data
return transfer
