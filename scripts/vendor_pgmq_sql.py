#!/usr/bin/env python3
"""Download pgmq.sql from a pinned PGMQ extension release.

The SQL file is not committed. CI, local tests, and package builds fetch it
from the extension tag recorded in ``src/pgmq/sql/VERSION``.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import urllib.error
import urllib.request

PGMQ_REPO = "pgmq/pgmq"
SQL_PATH = "pgmq-extension/sql/pgmq.sql"
PIN_PATH = "src/pgmq/sql/VERSION"
TARGET_PATH = "src/pgmq/sql/pgmq.sql"


def normalize_tag(tag: str) -> str:
    """Return a release tag with a leading ``v`` when missing."""
    tag = tag.strip()
    if not tag:
        raise ValueError("Release tag is required")
    return tag if tag.startswith("v") else f"v{tag}"


def normalize_version(tag: str) -> str:
    """Return the semver portion of a release tag."""
    return normalize_tag(tag).lstrip("v")


def read_pin(path: str | Path = PIN_PATH) -> str:
    """Return the pinned PGMQ extension version."""
    pin_path = Path(path)
    try:
        text = pin_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SystemExit(f"Failed to read SQL version pin {pin_path}: {exc}") from exc
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            return stripped
    raise SystemExit(f"SQL version pin {pin_path} is empty")


def write_pin(version: str, path: str | Path = PIN_PATH) -> None:
    """Write the pinned PGMQ extension version."""
    Path(path).write_text(f"{version}\n", encoding="utf-8")


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


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Download pgmq.sql from a PGMQ extension GitHub release. "
            "Without a tag, uses the version in src/pgmq/sql/VERSION."
        )
    )
    parser.add_argument(
        "tag",
        nargs="?",
        default=None,
        help="PGMQ extension release tag (for example v1.11.1 or 1.11.1)",
    )
    parser.add_argument(
        "--output",
        default=TARGET_PATH,
        help=f"Output path for downloaded SQL (default: {TARGET_PATH})",
    )
    parser.add_argument(
        "--pin-path",
        default=PIN_PATH,
        help=f"Path to the version pin file (default: {PIN_PATH})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Download even when the output file already exists",
    )
    parser.add_argument(
        "--update-pin",
        action="store_true",
        help="Write the resolved version to the pin file",
    )
    args = parser.parse_args()

    tag = args.tag or f"v{read_pin(args.pin_path)}"
    version = normalize_version(tag)
    if args.update_pin:
        write_pin(version, args.pin_path)
        print(f"Pinned PGMQ extension version {version} in {args.pin_path}")

    output = Path(args.output)
    if (
        args.tag is None
        and not args.force
        and output.exists()
        and output.stat().st_size > 0
    ):
        print(f"{output} already exists; skipping download (use --force to refresh)")
        return

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(fetch_sql(tag), encoding="utf-8")
    print(f"Downloaded pgmq.sql for PGMQ extension {version} to {output}")


if __name__ == "__main__":
    main()
