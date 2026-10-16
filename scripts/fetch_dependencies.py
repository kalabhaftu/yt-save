#!/usr/bin/env python3
"""Acquire the pinned Windows runtime files after verifying the archive SHA-256."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath, PureWindowsPath
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "dependencies.json"
USER_AGENT = "Yt-Save-dependency-fetcher/1.0"


def fetch(url: str, destination: Path, expected_hash: str, accept: str | None = None) -> None:
    parsed_url = urlsplit(url)
    if (
        parsed_url.scheme != "https"
        or parsed_url.hostname not in {"github.com", "api.github.com"}
        or parsed_url.username is not None
        or parsed_url.port is not None
    ):
        raise ValueError(f"unapproved dependency host in URL: {url}")
    headers = {"User-Agent": USER_AGENT}
    if accept:
        headers["Accept"] = accept
    request = urllib.request.Request(url, headers=headers)
    if parsed_url.hostname == "api.github.com":
        token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
        if token:
            # Unredirected headers authenticate the pinned API asset request but
            # urllib deliberately drops them when following GitHub's signed asset
            # redirect to a different host.
            request.add_unredirected_header("Authorization", f"Bearer {token}")
    digest = hashlib.sha256()
    try:
        with urllib.request.urlopen(request, timeout=60) as response, destination.open("wb") as output:
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
                digest.update(chunk)
    except Exception:
        destination.unlink(missing_ok=True)
        raise
    actual_hash = digest.hexdigest()
    if actual_hash.lower() != expected_hash.lower():
        destination.unlink(missing_ok=True)
        raise ValueError(
            f"SHA-256 mismatch for {url}: expected {expected_hash}, got {actual_hash}"
        )


def safe_name(name: str) -> PurePosixPath:
    normalized = PurePosixPath(name.replace("\\", "/"))
    if (
        normalized.is_absolute()
        or ".." in normalized.parts
        or PureWindowsPath(name).drive
    ):
        raise ValueError(f"unsafe archive entry path: {name}")
    return normalized


def extract_component(component: dict, archive_path: Path, output: Path) -> None:
    extract = component["extract"]
    if extract == "copy":
        shutil.copy2(archive_path, output / component["asset"])
        return

    with zipfile.ZipFile(archive_path) as archive:
        entries = [(entry, safe_name(entry.filename)) for entry in archive.infolist()]
        if extract == "deno":
            candidates = [entry for entry, name in entries if name.name.lower() == "deno.exe"]
            if len(candidates) != 1:
                raise ValueError(f"expected one deno.exe in {component['asset']}")
            (output / "deno.exe").write_bytes(archive.read(candidates[0]))
            for entry, name in entries:
                if name.name.lower() in {"license", "license.txt", "license.md", "third_party_licenses.txt"}:
                    (output / "licenses" / "deno").mkdir(parents=True, exist_ok=True)
                    destination = output / "licenses" / "deno" / name.name
                    destination.write_bytes(archive.read(entry))
            return

        if extract == "ffmpeg":
            copied = 0
            license_dir = output / "licenses" / "ffmpeg"
            for entry, name in entries:
                parts = tuple(part.lower() for part in name.parts)
                basename = name.name.lower()
                if "bin" in parts and basename.endswith((".exe", ".dll")):
                    if basename in {"ffmpeg.exe", "ffprobe.exe"} or basename.endswith(".dll"):
                        destination = output / name.name
                        destination.write_bytes(archive.read(entry))
                        copied += 1
                elif "license" in basename or basename.startswith("copying") or basename.endswith(".notice"):
                    license_dir.mkdir(parents=True, exist_ok=True)
                    (license_dir / name.name).write_bytes(archive.read(entry))
            if not (output / "ffmpeg.exe").is_file() or not (output / "ffprobe.exe").is_file():
                raise ValueError(f"ffmpeg archive did not contain ffmpeg.exe and ffprobe.exe ({copied} files copied)")
            return

    raise ValueError(f"unsupported extraction rule: {extract}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "target" / "package" / "bin")
    parser.add_argument("--component", action="append", help="fetch only this manifest component (repeatable)")
    args = parser.parse_args()

    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    args.output.mkdir(parents=True, exist_ok=True)
    selected = set(args.component or [])
    known = {component["id"] for component in manifest["components"]}
    unknown = selected - known
    if unknown:
        parser.error(f"unknown component id(s): {', '.join(sorted(unknown))}")

    for component in manifest["components"]:
        if selected and component["id"] not in selected:
            continue
        archive_path = args.output / f".{component['id']}.download"
        print(f"Fetching {component['id']} {component['version']} from its pinned upstream release…")
        try:
            fetch(component["url"], archive_path, component["sha256"], component.get("accept"))
            extract_component(component, archive_path, args.output)
        finally:
            archive_path.unlink(missing_ok=True)
        print(f"Verified and installed {component['id']} into {args.output}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # Keep the failure visible to local users and CI.
        print(f"dependency setup failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
