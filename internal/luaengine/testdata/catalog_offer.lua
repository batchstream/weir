local offer_string_fields = {"uid", "platform", "id", "name", "url", "country", "business_name", "bio"}
local offer_array_fields = {"hostnames"}
local offer_union_fields = {"uids"}
local offer_numeric_fields = {"sold", "follower_count", "product_count"}
local tracking_fields = {"ga_ua", "ga4", "gtm", "adsense", "facebook_pixel", "hotjar", "yandex_metrica", "facebook_domain_verification", "pinterest_domain_verify", "yandex_verification", "bing_site_verification", "script_urls", "inline_script_hashes"}
local offer_histories = {
    {field = "addresses", key = function(value) return (items(value.raw) or "") .. (items(value.country) or "") end, limit = 10},
    {field = "contacts", key = function(value) return (items(value.type) or "") .. (items(value.value) or "") end},
}

local function merge(current, incoming, observed_at)
    replace_strings(current, incoming, offer_string_fields)
    union(current, incoming, offer_union_fields)
    replace_arrays(current, incoming, offer_array_fields)
    for _, rule in ipairs(offer_histories) do history(current, incoming, rule) end
    if present(incoming.member_since) then current.member_since = incoming.member_since end
    nonzero(current, incoming, offer_numeric_fields)
    if present(incoming.tracking_ids) then
        current.tracking_ids = current.tracking_ids or weir.object()
        union(current.tracking_ids, incoming.tracking_ids, tracking_fields)
    end
    touch(current, incoming, observed_at)
    current.has_paypal = flag(incoming.has_paypal)
end

return function(current, incoming)
    assert(type(incoming.observed_at) == "string", "observed_at is required")
    if incoming.base and current then return current end
    local result = weir.object()
    if current then merge(result, current, incoming.observed_at) end
    merge(result, incoming.document, incoming.observed_at)
    result.evicted_at = nil
    return result
end
