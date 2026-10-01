"""Configuration YAML is written without a third-party Python dependency."""

import unittest

import config_yaml


class ConfigurationYAML(unittest.TestCase):
    def test_nested_routing_and_quoted_scalar_values(self):
        config = {
            "listeners": {"application": "0.0.0.0:7447"},
            "memory": "512MiB",
            "services": [{"name": "yes", "local": {"max_concurrency": 4}}],
            "diagnostics": {"allow_intranet": False},
        }
        expected = (
            '"listeners":\n'
            '  "application": "0.0.0.0:7447"\n'
            '"memory": "512MiB"\n'
            '"services":\n'
            '  - "name": "yes"\n'
            '    "local":\n'
            '      "max_concurrency": 4\n'
            '"diagnostics":\n'
            '  "allow_intranet": false\n'
        )
        self.assertEqual(config_yaml.dumps(config), expected)

    def test_strings_escape_yaml_syntax_and_keep_empty_containers(self):
        config = {"text": "line one\nquoted \"value\": #comment", "routes": [], "extra": {}}
        expected = (
            '"text": "line one\\nquoted \\"value\\": #comment"\n'
            '"routes": []\n'
            '"extra": {}\n'
        )
        self.assertEqual(config_yaml.dumps(config), expected)

    def test_unsupported_values_are_rejected(self):
        for value in (None, 1.5, {1: "value"}, {"value": None}, [1.5]):
            with self.subTest(value=value), self.assertRaises(TypeError):
                config_yaml.dumps(value)

    def test_unicode_is_written_without_json_surrogate_escapes(self):
        config = {"密码": "中文🚀"}
        self.assertEqual(config_yaml.dumps(config), '"密码": "中文🚀"\n')

    def test_yaml_line_breaks_and_control_characters_are_escaped(self):
        config = {"before\u0085after": "\u007f\u0090\u2028\u2029"}
        expected = '"before\\u0085after": "\\u007f\\u0090\\u2028\\u2029"\n'
        self.assertEqual(config_yaml.dumps(config), expected)


if __name__ == "__main__":
    unittest.main()
