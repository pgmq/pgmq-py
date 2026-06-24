#!/usr/bin/env python3
"""Check whether a PGMQ extension release should be vendored into pgmq-py."""

from __future__ import annotations

import argparse
import json
import os
import re
import urllib.error
import urllib.request

PGMQ_REPO = "pgmq/pgmq"
DEFAULT_BUNDLED_PATH = "src/pgmq/sql/pgmq.sql"
VERSION_MARKER_RE = re.compile(r"^-- pgmq-py bundled SQL version: (\S+)", re.MULTILINE)


def read_bundled_version(path: str) -> str | None:
    """Return the bundled SQL version marker, if present."""
    try:
        with open(path, encoding="utf-8") as handle:
            header = handle.read(256)
    except OSError:
        return None
    match = VERSION_MARKER_RE.search(header)
    return match.group(1) if match else None


def normalize_tag(tag: str) -> str:
    """Return a release tag with a leading ``v`` when missing."""
    tag = tag.strip()
    if not tag:
        raise ValueError("Release tag is required")
    return tag if tag.startswith("v") else f"v{tag}"


def fetch_latest_release_tag() -> str:
    """Return the latest non-prerelease tag from the PGMQ extension repo."""
    url = f"https://api.github.com/repos/{PGMQ_REPO}/releases/latest"
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "pgmq-py-vendor-script",
        },
    )
    try:
        payload = urllib.request.urlopen(request, timeout=60).read()
    except urllib.error.HTTPError as exc:
        raise SystemExit(
            f"Failed to fetch latest release from {url}: HTTP {exc.code}"
        ) from exc
    except urllib.error.URLError as exc:
        raise SystemExit(
            f"Failed to fetch latest release from {url}: {exc.reason}"
        ) from exc

    data = json.loads(payload.decode("utf-8"))
    tag_name = data.get("tag_name")
    if not tag_name:
        raise SystemExit("GitHub latest-release response did not include tag_name")
    return normalize_tag(tag_name)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Determine whether pgmq.sql should be vendored from pgmq/pgmq."
    )
    parser.add_argument(
        "--bundled-path",
        default=DEFAULT_BUNDLED_PATH,
        help=f"Path to bundled SQL (default: {DEFAULT_BUNDLED_PATH})",
    )
    parser.add_argument(
        "--tag",
        help="Explicit PGMQ extension release tag (default: latest GitHub release)",
    )
    parser.add_argument(
        "--trigger",
        default="manual",
        help="Workflow trigger name for logging and PR bodies",
    )
    parser.add_argument(
        "--github-output",
        action="store_true",
        help="Append results to the GITHUB_OUTPUT file",
    )
    args = parser.parse_args()

    tag = normalize_tag(args.tag) if args.tag else fetch_latest_release_tag()
    version = tag.lstrip("v")
    bundled_version = read_bundled_version(args.bundled_path)
    needs_update = bundled_version != version

    print(f"Trigger: {args.trigger}")
    print(f"Extension tag: {tag}")
    print(f"Bundled version: {bundled_version or 'missing'}")
    print(f"Needs update: {needs_update}")

    if args.github_output:
        github_output = os.environ.get("GITHUB_OUTPUT")
        if not github_output:
            raise SystemExit("GITHUB_OUTPUT is not set")
        with open(github_output, "a", encoding="utf-8") as handle:
            handle.write(f"tag={tag}\n")
            handle.write(f"version={version}\n")
            handle.write(f"needs_update={'true' if needs_update else 'false'}\n")
            handle.write(f"trigger={args.trigger}\n")


if __name__ == "__main__":
    main()
