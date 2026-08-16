#!/usr/bin/env python3
"""Check whether the pinned PGMQ SQL version should be updated."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import urllib.error
import urllib.request

PGMQ_REPO = "pgmq/pgmq"
DEFAULT_PIN_PATH = "src/pgmq/sql/VERSION"


def read_pinned_version(path: str) -> str | None:
    """Return the pinned SQL version, if present."""
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError:
        return None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            return stripped
    return None


def normalize_tag(tag: str) -> str:
    """Return a release tag with a leading ``v`` when missing."""
    tag = tag.strip()
    if not tag:
        raise ValueError("Release tag is required")
    return tag if tag.startswith("v") else f"v{tag}"


def fetch_latest_release_tag() -> str:
    """Return the latest non-prerelease tag from the PGMQ extension repo."""
    url = f"https://api.github.com/repos/{PGMQ_REPO}/releases/latest"
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "pgmq-py-vendor-script",
    }
    token = os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
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

    try:
        data = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SystemExit(f"Failed to parse GitHub release response: {exc}") from exc
    tag_name = data.get("tag_name")
    if not tag_name:
        raise SystemExit("GitHub latest-release response did not include tag_name")
    return normalize_tag(tag_name)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Determine whether the pinned pgmq.sql version should change."
    )
    parser.add_argument(
        "--pin-path",
        default=DEFAULT_PIN_PATH,
        help=f"Path to the version pin file (default: {DEFAULT_PIN_PATH})",
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
    pinned_version = read_pinned_version(args.pin_path)
    needs_update = pinned_version != version

    print(f"Trigger: {args.trigger}")
    print(f"Extension tag: {tag}")
    print(f"Pinned version: {pinned_version or 'missing'}")
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
