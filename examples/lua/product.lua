local function union_strings(left, right)
  local result = weir.array()
  local seen = {}
  for _, source in ipairs({left or {}, right or {}}) do
    for _, item in ipairs(source) do
      if type(item) == "string" and item ~= "" and not seen[item] then
        seen[item] = true
        result[#result + 1] = item
      end
    end
  end
  return result
end

return function(current, incoming)
  current = current or weir.object()
  incoming = incoming or weir.object()
  if type(incoming.title) == "string" and incoming.title ~= "" then
    current.title = incoming.title
  end
  current.tags = union_strings(current.tags, incoming.tags)
  current.updated_at = weir.time.now()
  return current
end
