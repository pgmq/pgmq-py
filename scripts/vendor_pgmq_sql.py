#!/usr/bin/env python3
"""Download pgmq.sql from a PGMQ extension release and update the bundled copy."""

from __future__ import annotations

import argparse
import re
import urllib.error
import urllib.request

PGMQ_REPO = "pgmq/pgmq"
SQL_PATH = "pgmq-extension/sql/pgmq.sql"
TARGET_PATH = "src/pgmq/sql/pgmq.sql"
VERSION_MARKER_RE = re.compile(r"^-- pgmq-py bundled SQL version: \S+\n", re.MULTILINE)


def normalize_tag(tag: str) -> str:
    """Return a release tag with a leading ``v`` when missing."""
    tag = tag.strip()
    if not tag:
        raise ValueError("Release tag is required")
    return tag if tag.startswith("v") else f"v{tag}"


def normalize_version(tag: str) -> str:
    """Return the semver portion of a release tag."""
    return normalize_tag(tag).lstrip("v")


def build_raw_url(tag: str) -> str:
    """Build the raw GitHub URL for ``pgmq.sql`` at a release tag."""
    release_tag = normalize_tag(tag)
    return (
        f"https://raw.githubusercontent.com/{PGMQ_REPO}/refs/tags/"
        f"{release_tag}/{SQL_PATH}"
    )


def fetch_sql(tag: str) -> str:
    """Download the SQL install script for a PGMQ extension release."""
    url = build_raw_url(tag)
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "pgmq-py-vendor-script"},
    )
    try:
        payload = urllib.request.urlopen(request, timeout=60).read()
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"Failed to fetch {url}: HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"Failed to fetch {url}: {exc.reason}") from exc

    try:
        sql_content = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SystemExit(f"Failed to decode SQL downloaded from {url}: {exc}") from exc

    if not sql_content.strip():
        raise SystemExit(f"Downloaded SQL from {url} is empty")
    return sql_content


def apply_version_marker(sql_content: str, version: str) -> str:
    """Ensure the bundled SQL begins with the pgmq-py version marker."""
    body = VERSION_MARKER_RE.sub("", sql_content).lstrip("\n")
    return f"-- pgmq-py bundled SQL version: {version}\n{body}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Vendor pgmq.sql from a PGMQ extension GitHub release."
    )
    parser.add_argument(
        "tag",
        help="PGMQ extension release tag (for example v1.11.1 or 1.11.1)",
    )
    parser.add_argument(
        "--output",
        default=TARGET_PATH,
        help=f"Output path for bundled SQL (default: {TARGET_PATH})",
    )
    args = parser.parse_args()

    version = normalize_version(args.tag)
    bundled_sql = apply_version_marker(fetch_sql(args.tag), version)
    with open(args.output, "w", encoding="utf-8") as handle:
        handle.write(bundled_sql)
    print(f"Vendored pgmq.sql for PGMQ extension {version} to {args.output}")


if __name__ == "__main__":
    main()
