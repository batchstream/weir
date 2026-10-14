-- Business-rule fixture translated from PSE main 283940ef442b479440e45b39c86988351b92d7b5.
-- Helpers below are local policy; only Weir's public value/Unicode primitives are used.
local function present(value)
    return value ~= nil and weir.kind(value) ~= "null"
end

local function items(value)
    if not present(value) then return nil end
    return value
end

local function flag(value)
    if present(value) then assert(type(value) == "boolean", "invalid boolean field") end
    return value == true or nil
end

local function append_all(current, incoming)
    if incoming then
        for _, value in ipairs(incoming) do current[#current + 1] = value end
    end
end

local function keep_tail(values, limit)
    local result = weir.array()
    for index = math.max(1, #values - limit + 1), #values do
        result[#result + 1] = values[index]
    end
    return result
end

local function deduplicate(values, make_key)
    local result, seen = weir.array(), {}
    for _, value in ipairs(values) do
        local identity = make_key(value)
        if not seen[identity] then
            result[#result + 1] = value
            seen[identity] = true
        end
    end
    return result
end

local function union_strings(current, incoming)
    local values = weir.array()
    append_all(values, current)
    append_all(values, incoming)
    return deduplicate(values, function(value)
        assert(type(value) == "string", "invalid string union item")
        return value
    end)
end

local function replace_nonempty_string(current, incoming, field)
    local value = incoming[field]
    assert(type(value) == "string", "invalid string field")
    if value ~= "" then current[field] = value end
end

local function replace_nonempty_array(current, incoming, field)
    local value = incoming[field]
    assert(weir.kind(value) == "array", "invalid array field")
    if #value > 0 then current[field] = value end
end

-- Length-delimited keys preserve scalar types, container types and map-order independence.
local function key(value)
    if not present(value) then return "null" end
    local kind = type(value)
    if kind == "string" then return "s" .. #value .. ":" .. value end
    if kind == "boolean" then return tostring(value) end
    if kind == "number" then
        if math.type(value) == "integer" then return "n" .. tostring(value) end
        return "n" .. string.format("%.17g", value)
    end
    if kind == "userdata" then
        local native_type, data = weir.data(value)
        assert(native_type == "weir.json.number.v1", "invalid JSON scalar")
        return "n" .. data
    end
    assert(kind == "table", "invalid JSON value")
    local keys = {}
    for k in pairs(value) do keys[#keys + 1] = k end
    table.sort(keys)
    local parts = {(weir.kind(value) == "array" and "a" or "o") .. #keys .. ":"}
    for _, k in ipairs(keys) do
        parts[#parts + 1] = key(k)
        parts[#parts + 1] = key(value[k])
    end
    return table.concat(parts)
end

local function replace_strings(current, incoming, fields)
    for _, field in ipairs(fields) do
        if present(incoming[field]) then replace_nonempty_string(current, incoming, field) end
    end
end

local function replace_arrays(current, incoming, fields)
    for _, field in ipairs(fields) do
        if present(incoming[field]) then replace_nonempty_array(current, incoming, field) end
    end
end

local function union(current, incoming, fields)
    for _, field in ipairs(fields) do
        local values = items(incoming[field])
        if values and #values > 0 then
            current[field] = union_strings(items(current[field]), values)
        end
    end
end

local function history(current, incoming, rule)
    local field, make_key, limit = rule.field, rule.key, rule.limit
    local values = items(current[field]) or weir.array()
    local extra = items(incoming[field])
    if extra and #extra > 0 then
        append_all(values, extra)
        values = deduplicate(values, make_key)
    end
    if limit then values = keep_tail(values, limit) end
    if #values > 0 then current[field] = values else current[field] = nil end
end

local function touch(current, incoming, observed_at)
    if not present(current.first_found_at) then
        current.first_found_at = items(incoming.first_found_at) or observed_at
    end
    current.last_found_at = observed_at
end

local function nonzero(current, incoming, fields)
    for _, field in ipairs(fields) do
        local value = incoming[field]
        if present(value) then
            assert(type(value) == "number", "invalid numeric field " .. field)
            if value ~= 0 then current[field] = value end
        end
    end
end
