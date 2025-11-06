-- Common helper functions injected into Lua Beetle scripts at load time.
-- These helpers are appended ahead of every script by the test harness so
-- we can share optimized utilities without relying on Redis include support.

local lb_zero_padding = string.rep('\0', 127)
local lb_zero_16 = string.rep('\0', 16)
local lb_result_cache = {}

function lb_result(code)
    local cached = lb_result_cache[code]
    if cached then
        return cached
    end

    local value = string.char(code) .. lb_zero_padding
    lb_result_cache[code] = value
    return value
end

local lb_digits = "0123456789abcdef"
local lb_hex_lookup = {}
for i = 0, 255 do
    local hi = math.floor(i / 16)
    local lo = i % 16
    lb_hex_lookup[i] =
        string.sub(lb_digits, hi + 1, hi + 1) ..
        string.sub(lb_digits, lo + 1, lo + 1)
end

function lb_has_flag(value, mask)
    return value % (mask * 2) >= mask
end

function lb_hex16(bytes)
    local out = {}
    for i = 1, 16 do
        out[i] = lb_hex_lookup[string.byte(bytes, i)]
    end
    return table.concat(out)
end

function lb_slice16(data, offset)
    return string.sub(data, offset, offset + 15)
end

function lb_encode_u64(value)
    local bytes = {}
    local v = value
    for i = 1, 8 do
        bytes[i] = string.char(v % 256)
        v = math.floor(v / 256)
    end
    return table.concat(bytes)
end

function lb_decode_u16(data, offset)
    return string.byte(data, offset) + string.byte(data, offset + 1) * 256
end

function lb_decode_u32(data, offset)
    local b1 = string.byte(data, offset)
    local b2 = string.byte(data, offset + 1)
    local b3 = string.byte(data, offset + 2)
    local b4 = string.byte(data, offset + 3)
    return b1 + b2 * 256 + b3 * 65536 + b4 * 16777216
end

function lb_decode_u64(data, offset)
    local low = lb_decode_u32(data, offset)
    local high = lb_decode_u32(data, offset + 4)
    return low + high * 4294967296
end

function lb_decode_u128(data, offset)
    return {
        low = lb_decode_u64(data, offset),
        high = lb_decode_u64(data, offset + 8),
    }
end

local lb_tmp_bytes = {}

function lb_add_u128(left, right)
    local carry = 0
    for i = 1, 16 do
        local sum = string.byte(left, i) + string.byte(right, i) + carry
        lb_tmp_bytes[i] = string.char(sum % 256)
        carry = math.floor(sum / 256)
    end
    return table.concat(lb_tmp_bytes, "", 1, 16), carry
end

function lb_sub_u128(left, right)
    local borrow = 0
    for i = 1, 16 do
        local diff = string.byte(left, i) - string.byte(right, i) - borrow
        if diff < 0 then
            diff = diff + 256
            borrow = 1
        else
            borrow = 0
        end
        lb_tmp_bytes[i] = string.char(diff)
    end
    if borrow ~= 0 then
        return nil
    end
    return table.concat(lb_tmp_bytes, "", 1, 16)
end

function lb_add_field(data, offset, value_bytes)
    local current = string.sub(data, offset, offset + 15)
    local sum, carry = lb_add_u128(current, value_bytes)
    if carry ~= 0 then
        return nil
    end
    return sum
end

function lb_sub_field(data, offset, value_bytes)
    return lb_sub_u128(string.sub(data, offset, offset + 15), value_bytes)
end

function lb_compare_u128(left, right)
    for i = 16, 1, -1 do
        local a = string.byte(left, i)
        local b = string.byte(right, i)
        if a ~= b then
            if a > b then
                return 1
            else
                return -1
            end
        end
    end
    return 0
end
