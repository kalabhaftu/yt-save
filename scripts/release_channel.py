#!/usr/bin/env python3
"""Validate a v-prefixed SemVer release tag and identify its channel."""

from __future__ import annotations

import argparse
import json
import re

_NUMBER = r"(?:0|[1-9][0-9]*)"
_PRE_RELEASE_ID = rf"(?:{_NUMBER}|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
_BUILD_ID = r"[0-9A-Za-z-]+"
_TAG = re.compile(
    rf"^v(?P<version>{_NUMBER}\.{_NUMBER}\.{_NUMBER}"
    rf"(?P<prerelease>-(?:{_PRE_RELEASE_ID})(?:\.{_PRE_RELEASE_ID})*)?"
    rf"(?:\+(?:{_BUILD_ID})(?:\.{_BUILD_ID})*)?)$"
)


def classify_release_tag(tag: str) -> dict[str, str]:
    """Return the version without ``v`` and its stable/preview channel."""
    match = _TAG.fullmatch(tag)
    if match is None:
        raise ValueError(
            "release tag must be vMAJOR.MINOR.PATCH, optionally followed by a "
            "valid SemVer prerelease or build suffix"
        )
    return {
        "version": match.group("version"),
        "channel": "preview" if match.group("prerelease") else "stable",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True, help="v-prefixed SemVer version tag")
    args = parser.parse_args()
    try:
        result = classify_release_tag(args.tag)
    except ValueError as error:
        parser.error(str(error))
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
