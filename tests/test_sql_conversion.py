"""
Unit tests for SQL parameter conversion utilities.

These tests do not require a running database.
"""

import unittest

from pgmq._sql import (
    _iter_placeholders,
    convert_sql_params,
    _convert_psycopg_to_asyncpg,
    SEND,
    SEND_BATCH,
)


class TestIterPlaceholders(unittest.TestCase):
    def test_simple(self):
        sql = "SELECT %s, %s"
        self.assertEqual(list(_iter_placeholders(sql)), [(7, 9), (11, 13)])

    def test_no_placeholders(self):
        sql = "SELECT 1"
        self.assertEqual(list(_iter_placeholders(sql)), [])

    def test_inside_string_literal(self):
        sql = "SELECT '%s'"
        self.assertEqual(list(_iter_placeholders(sql)), [])

    def test_inside_string_with_escaped_quote(self):
        sql = "SELECT 'it''s %s here'"
        self.assertEqual(list(_iter_placeholders(sql)), [])

    def test_mixed_literal_and_real(self):
        sql = "SELECT '%s', %s"
        self.assertEqual(list(_iter_placeholders(sql)), [(13, 15)])

    def test_placeholder_after_literal(self):
        sql = "SELECT 'foo' WHERE bar = %s"
        self.assertEqual(list(_iter_placeholders(sql)), [(25, 27)])


class TestConvertSqlParams(unittest.TestCase):
    def test_basic(self):
        sql = "SELECT %s, %s"
        result, params = convert_sql_params(sql, (1, 2))
        self.assertEqual(result, r"SELECT :param_1, :param_2")
        self.assertEqual(params, {"param_1": 1, "param_2": 2})

    def test_no_params(self):
        sql = "SELECT 1"
        result, params = convert_sql_params(sql)
        self.assertEqual(result, sql)
        self.assertEqual(params, {})

    def test_empty_tuple_params(self):
        sql = "SELECT 1"
        result, params = convert_sql_params(sql, ())
        self.assertEqual(result, sql)
        self.assertEqual(params, {})

    def test_count_mismatch(self):
        sql = "SELECT %s, %s"
        with self.assertRaises(ValueError) as ctx:
            convert_sql_params(sql, (1,))
        self.assertIn("Parameter count mismatch", str(ctx.exception))

    def test_jsonb_dict(self):
        sql = SEND  # "SELECT * FROM pgmq.send(queue_name=>%s::text, msg=>%s::jsonb);"
        result, params = convert_sql_params(sql, ("my_queue", {"key": "value"}))
        self.assertIn(":param_1", result)
        self.assertIn(":param_2", result)
        self.assertEqual(params["param_1"], "my_queue")
        self.assertEqual(params["param_2"], '{"key": "value"}')

    def test_jsonb_array(self):
        sql = SEND_BATCH  # "SELECT * FROM pgmq.send_batch(queue_name=>%s::text, msgs=>%s::jsonb[]);"
        result, params = convert_sql_params(sql, ("my_queue", [{"a": 1}, {"b": 2}]))
        self.assertEqual(params["param_2"], ['{"a": 1}', '{"b": 2}'])

    def test_jsonb_array_with_scalars(self):
        sql = SEND_BATCH
        result, params = convert_sql_params(sql, ("my_queue", [1, 2, 3]))
        self.assertEqual(params["param_2"], [1, 2, 3])

    def test_ignores_placeholder_in_string_literal(self):
        sql = "SELECT '%s' WHERE x = %s"
        result, params = convert_sql_params(sql, (42,))
        self.assertEqual(result, r"SELECT '%s' WHERE x = :param_1")
        self.assertEqual(params, {"param_1": 42})

    def test_whitespace_tolerant_jsonb_cast(self):
        sql = "SELECT func(msg=>%s :: jsonb)"
        result, params = convert_sql_params(sql, ({"k": "v"},))
        self.assertEqual(params["param_1"], '{"k": "v"}')

    def test_whitespace_tolerant_jsonb_array_cast(self):
        sql = "SELECT func(msgs=>%s :: jsonb [])"
        result, params = convert_sql_params(sql, ([{"k": "v"}],))
        self.assertEqual(params["param_1"], ['{"k": "v"}'])

    def test_cast_operator_escaped(self):
        sql = "SELECT %s::text"
        result, _ = convert_sql_params(sql, ("val",))
        self.assertEqual(result, r"SELECT :param_1\:\:text")


class TestConvertPsycopgToAsyncpg(unittest.TestCase):
    def test_basic(self):
        sql = "SELECT %s, %s"
        result = _convert_psycopg_to_asyncpg(sql)
        self.assertEqual(result, "SELECT $1, $2")

    def test_no_placeholders(self):
        sql = "SELECT 1"
        result = _convert_psycopg_to_asyncpg(sql)
        self.assertEqual(result, sql)

    def test_ignores_placeholder_in_string_literal(self):
        sql = "SELECT '%s' WHERE x = %s"
        result = _convert_psycopg_to_asyncpg(sql)
        self.assertEqual(result, "SELECT '%s' WHERE x = $1")

    def test_all_sql_constants_convert(self):
        """Ensure every SQL constant in _sql.py can be converted without error."""
        from pgmq import _sql

        for name in dir(_sql):
            if name.isupper() and isinstance(getattr(_sql, name), str):
                original = getattr(_sql, name)
                converted = _convert_psycopg_to_asyncpg(original)
                # Placeholder counts should match
                self.assertEqual(
                    original.count("%s"),
                    converted.count("$"),
                    f"Mismatch for {name}",
                )


if __name__ == "__main__":
    unittest.main()
