"""Write the block YAML used by owned Weir configuration fixtures."""

import json

_YAML_ESCAPES = {
    codepoint: "\\u%04x" % codepoint
    for codepoint in (*range(0x7F, 0xA0), 0x2028, 0x2029)
}


def _quote(value):
    # Keep Unicode readable without JSON surrogate escapes or YAML line folding.
    return json.dumps(value, ensure_ascii=False).translate(_YAML_ESCAPES)


def dumps(value):
    return "\n".join(_lines(value, 0)) + "\n"


def _scalar(value):
    if type(value) is str:
        return _quote(value)
    if type(value) in (bool, int):
        return json.dumps(value)
    if type(value) in (dict, list) and not value:
        return json.dumps(value)
    raise TypeError("configuration YAML supports dict, list, str, bool and int")


def _lines(value, depth):
    prefix = "  " * depth
    lines = []
    if type(value) is dict and value:
        for key, item in value.items():
            if type(key) is not str:
                raise TypeError("configuration YAML keys must be strings")
            field = prefix + _quote(key) + ":"
            if type(item) in (dict, list) and item:
                lines.append(field)
                lines.extend(_lines(item, depth + 1))
            else:
                lines.append(field + " " + _scalar(item))
    elif type(value) is list and value:
        for item in value:
            child = _lines(item, depth + 1)
            lines.append(prefix + "- " + child[0].lstrip())
            lines.extend(child[1:])
    else:
        lines.append(prefix + _scalar(value))
    return lines
