
local function sold_key(value)
    local date = items(value.record_at) or ""
    return tostring(items(value.sold) or 0) .. "-" .. tostring(items(value.period_hours) or 0) .. "-" .. string.sub(date, 1, 10)
end

local function stock_key(value)
    local variables = items(value.variables)
    local suffix = ""
    if variables and next(variables) ~= nil then suffix = key(variables) end
    return tostring(items(value.stock) or 0) .. ":" .. suffix
end

local comment_string_fields = {"title", "description", "customer_name", "customer_profile", "location", "currency"}
local comment_numeric_fields = {"score", "customer_rating_score", "price"}

-- These are the finite RFC3339 rules used by the business key, expressed without
-- exposing an uncancellable native pattern matcher to the VM.
local function comment_date(date)
    local offset = string.sub(date, -6)
    if offset == "+00:00" or offset == "-00:00" then
        date = string.sub(date, 1, #date - 6) .. "Z"
    end
    for start = 1, #date do
        if string.byte(date, start) == 46 then
            local finish = start + 1
            while finish <= #date do
                local byte = string.byte(date, finish)
                if byte < 48 or byte > 57 then break end
                finish = finish + 1
            end
            local zone = string.byte(date, finish)
            if finish > start + 1 and (zone == 90 or zone == 43 or zone == 45) then
                local tail = finish - 1
                while tail > start and string.byte(date, tail) == 48 do tail = tail - 1 end
                local fraction = ""
                if tail > start then fraction = string.sub(date, start, tail) end
                return string.sub(date, 1, start - 1) .. fraction .. string.sub(date, finish)
            end
        end
    end
    return date
end

local function comment_key(value)
    local normalized = weir.object()
    replace_strings(normalized, value, comment_string_fields)
    nonzero(normalized, value, comment_numeric_fields)
    if present(value.date) then
        normalized.date = comment_date(value.date)
    end
    return key(normalized)
end

local function hostname(value)
    local text = items(value) or ""
    local first = string.byte(text, 1) or 0
    if not ((first >= 65 and first <= 90) or (first >= 97 and first <= 122)) then return "" end
    local colon = 2
    while colon <= #text do
        local byte = string.byte(text, colon)
        if byte == 58 then break end
        if not ((byte >= 65 and byte <= 90) or (byte >= 97 and byte <= 122) or
            (byte >= 48 and byte <= 57) or byte == 43 or byte == 46 or byte == 45) then return "" end
        colon = colon + 1
    end
    if string.sub(text, colon, colon + 2) ~= "://" then return "" end
    local finish = colon + 3
    while finish <= #text do
        local byte = string.byte(text, finish)
        if byte == 47 or byte == 63 or byte == 35 then break end
        finish = finish + 1
    end
    local authority = string.sub(text, colon + 3, finish - 1)
    local last_at = 0
    for index = 1, #authority do
        if string.byte(authority, index) == 64 then last_at = index end
    end
    authority = string.sub(authority, last_at + 1)
    if string.byte(authority, 1) == 91 then
        for index = 3, #authority do
            if string.byte(authority, index) == 93 then return string.sub(authority, 2, index - 1) end
        end
    end
    local tail = #authority
    while tail > 0 do
        local byte = string.byte(authority, tail)
        if byte < 48 or byte > 57 then break end
        tail = tail - 1
    end
    if tail < #authority and string.byte(authority, tail) == 58 then
        authority = string.sub(authority, 1, tail - 1)
    end
    return authority
end

local function reconcile(current, incoming)
    local document = incoming.document
    local urls = incoming.offer_product_urls or {}
    for _, old in ipairs(items(current.offers) or {}) do
        if current.url == incoming.offer_product_url_base and (items(old.product_url) or "") == "" and urls[old.uid] then
            old.product_url = urls[old.uid]
        end
        if current.platform == document.platform then
            local found = false
            for _, new in ipairs(items(document.offers) or {}) do
                if old.id == new.id then
                    old.product_price = new.product_price
                    found = true
                    break
                end
                if hostname(old.url) ~= hostname(new.url) then
                    found = true
                    break
                end
            end
            if found then old.expiry_detected_at = weir.null()
            else old.expiry_detected_at = incoming.observed_at end
        end
    end
    touch(current, current, incoming.observed_at)
    return current
end

local product_string_fields = {"uid", "platform", "country", "id", "url", "title", "description", "condition", "ecommerce_class", "translated_text", "serial_number"}
local product_array_fields = {"category", "gallery", "prices", "hostnames", "pto_class", "brands"}
local product_union_fields = {"allowed_countries", "restricted_countries", "languages", "countries_from_ip"}
local product_numeric_fields = {"comment_count"}
local product_histories = {
    {field = "solds", key = sold_key, limit = 20},
    {field = "stocks", key = stock_key, limit = 20},
    {field = "comments", key = comment_key, limit = 20},
}

local function merge(current, incoming, observed_at)
    local uids = weir.array()
    if (items(current.uid) or "") ~= "" then uids[#uids + 1] = current.uid end
    if (items(incoming.uid) or "") ~= "" then uids[#uids + 1] = incoming.uid end
    uids = union_strings(uids, items(incoming.uids))
    local combined_uids = union_strings(items(current.uids), uids)
    current.uids = weir.array()
    for _, uid in ipairs(combined_uids) do
        if uid ~= "" then current.uids[#current.uids + 1] = uid end
    end
    replace_strings(current, incoming, product_string_fields)
    replace_arrays(current, incoming, product_array_fields)
    if present(incoming.brand) and incoming.brand ~= "" then current.brand = utf8.upper(incoming.brand) end
    for _, rule in ipairs(product_histories) do history(current, incoming, rule) end
    nonzero(current, incoming, product_numeric_fields)
    if present(incoming.rating) then current.rating = incoming.rating end
    local offers = items(incoming.offers)
    if offers and #offers > 0 then
        local combined = weir.array()
        append_all(combined, offers)
        append_all(combined, items(current.offers))
        current.offers = deduplicate(combined, function(offer) return items(offer.uid) or "" end)
    end
    if present(current.offers) then current.offers = keep_tail(current.offers, 50) end
    union(current, incoming, product_union_fields)
    touch(current, incoming, observed_at)
    current.sold_by_platform = flag(incoming.sold_by_platform)
    current.available = flag(incoming.available)
end

return function(current, incoming)
    assert(type(incoming.observed_at) == "string", "observed_at is required")
    if incoming.patch then
        if current == nil then
            current = weir.object()
            merge(current, incoming.document, incoming.observed_at)
        end
        for field, value in pairs(incoming.patch) do current[field] = value end
        return current
    end
    if incoming.reconcile_offers and current then
        return reconcile(current, incoming)
    end
    local result = weir.object()
    if current then merge(result, current, incoming.observed_at) end
    merge(result, incoming.document, incoming.observed_at)
    result.evicted_at = nil
    return result
end
