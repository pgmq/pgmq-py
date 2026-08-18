"""Unit tests for PGMQ SQL vendor scripts (no database or network)."""

import importlib.util
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_module(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestVendorPgmqSql(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = _load_module("vendor_pgmq_sql", "scripts/vendor_pgmq_sql.py")

    def test_normalize_tag_adds_v_prefix(self):
        self.assertEqual(self.mod.normalize_tag("1.11.1"), "v1.11.1")
        self.assertEqual(self.mod.normalize_tag("v1.11.1"), "v1.11.1")

    def test_normalize_version(self):
        self.assertEqual(self.mod.normalize_version("v1.11.1"), "1.11.1")
        self.assertEqual(self.mod.normalize_version("1.11.1"), "1.11.1")

    def test_build_raw_url_uses_extension_release_tag(self):
        self.assertEqual(
            self.mod.build_raw_url("v1.11.1"),
            "https://raw.githubusercontent.com/pgmq/pgmq/refs/tags/"
            "v1.11.1/pgmq-extension/sql/pgmq.sql",
        )

    def test_read_and_write_pin(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "VERSION"
            self.mod.write_pin("1.11.1", path)
            self.assertEqual(self.mod.read_pin(path), "1.11.1")

    def test_sql_matches_pin_requires_stamp(self):
        with tempfile.TemporaryDirectory() as tmp:
            sql_path = Path(tmp) / "pgmq.sql"
            sql_path.write_text("CREATE SCHEMA IF NOT EXISTS pgmq;", encoding="utf-8")
            self.assertFalse(self.mod.sql_matches_pin(sql_path, "1.11.1"))
            self.mod.write_stamp(sql_path, "1.11.1")
            self.assertTrue(self.mod.sql_matches_pin(sql_path, "1.11.1"))
            self.assertFalse(self.mod.sql_matches_pin(sql_path, "1.12.0"))

    def test_check_dist_requires_sql(self):
        import zipfile

        with tempfile.TemporaryDirectory() as tmp:
            dist = Path(tmp)
            empty_wheel = dist / "pgmq-0.0.0-py3-none-any.whl"
            with zipfile.ZipFile(empty_wheel, "w") as handle:
                handle.writestr("pgmq/sql/VERSION", "1.11.1\n")
            with self.assertRaises(SystemExit):
                self.mod.check_dist(dist)

            empty_wheel.unlink()
            full_wheel = dist / "pgmq-0.0.1-py3-none-any.whl"
            with zipfile.ZipFile(full_wheel, "w") as handle:
                handle.writestr("pgmq/sql/VERSION", "1.11.1\n")
                handle.writestr(
                    "pgmq/sql/pgmq.sql", "CREATE SCHEMA IF NOT EXISTS pgmq;"
                )
            self.mod.check_dist(dist)


class TestExtensionReleaseStatus(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = _load_module(
            "pgmq_extension_release_status",
            "scripts/pgmq_extension_release_status.py",
        )

    def test_read_pinned_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "VERSION"
            path.write_text("# comment\n1.11.1\n", encoding="utf-8")
            self.assertEqual(self.mod.read_pinned_version(str(path)), "1.11.1")

    def test_read_pinned_version_missing(self):
        self.assertIsNone(self.mod.read_pinned_version("/no/such/VERSION"))
