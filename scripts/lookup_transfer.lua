-- Lookup Transfer Script (Binary Encoding)
-- Returns the 128-byte binary transfer data
-- KEYS: none
-- ARGV[1]: transfer ID (16 bytes binary)

-- Helper to convert 16-byte ID to hex string for Redis keys
local function id_to_string(id_bytes)
    local hex = ""
    for i = 1, #id_bytes do
        hex = hex .. string.format("%02x", string.byte(id_bytes, i))
    end
    return hex
end

local transfer_id_raw = ARGV[1]

-- Validate ID size
if #transfer_id_raw ~= 16 then
    return "" -- Empty response for invalid ID
end

local transfer_id = id_to_string(transfer_id_raw)
local key = "transfer:" .. transfer_id

-- Get transfer data
local transfer = redis.call('GET', key)

if not transfer or #transfer ~= 128 then
    return "" -- Return empty for non-existent transfer
end

-- Return the raw 128-byte binary data
return transfer
