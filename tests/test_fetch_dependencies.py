from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from urllib.request import HTTPRedirectHandler
from unittest.mock import patch

from scripts.fetch_dependencies import MANIFEST, extract_component, fetch, safe_name, validate_manifest


class FakeResponse(io.BytesIO):
    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class DependencyFetcherTests(unittest.TestCase):
    def test_safe_name_rejects_traversal_and_windows_absolute_paths(self) -> None:
        for name in ("../outside.exe", "/absolute/file", "C:\\outside.exe"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                safe_name(name)

        self.assertEqual(safe_name("bin/deno.exe").as_posix(), "bin/deno.exe")

    def test_fetch_requires_exact_https_github_host(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            target = Path(temporary_directory) / "asset.zip"
            for url in (
                "http://github.com/vendor/release/file.zip",
                "https://github.com.attacker.invalid/vendor/file.zip",
                "https://user@github.com/vendor/file.zip",
            ):
                with self.subTest(url=url), self.assertRaises(ValueError):
                    fetch(url, target, "00" * 32)
            self.assertFalse(target.exists())

    def test_fetch_verifies_sha256_and_removes_mismatched_download(self) -> None:
        content = b"pinned release bytes"
        expected = hashlib.sha256(content).hexdigest()
        with tempfile.TemporaryDirectory() as temporary_directory:
            target = Path(temporary_directory) / "asset.zip"
            with patch("scripts.fetch_dependencies.urllib.request.urlopen", return_value=FakeResponse(content)):
                fetch("https://github.com/vendor/project/releases/file.zip", target, expected)
            self.assertEqual(target.read_bytes(), content)

            with patch("scripts.fetch_dependencies.urllib.request.urlopen", return_value=FakeResponse(content)):
                with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                    fetch("https://github.com/vendor/project/releases/file.zip", target, "00" * 32)
            self.assertFalse(target.exists())

    def test_fetch_supports_pinned_github_api_asset_ids(self) -> None:
        content = b"immutable github release asset"
        expected = hashlib.sha256(content).hexdigest()
        with tempfile.TemporaryDirectory() as temporary_directory:
            target = Path(temporary_directory) / "asset.zip"
            with patch(
                "scripts.fetch_dependencies.urllib.request.urlopen",
                return_value=FakeResponse(content),
            ) as open_url, patch.dict("os.environ", {"GH_TOKEN": "test-token"}):
                fetch(
                    "https://api.github.com/repos/vendor/project/releases/assets/123",
                    target,
                    expected,
                    "application/octet-stream",
                )
            request = open_url.call_args.args[0]
            self.assertEqual(request.get_header("Accept"), "application/octet-stream")
            self.assertEqual(request.unredirected_hdrs.get("Authorization"), "Bearer test-token")
            redirected = HTTPRedirectHandler().redirect_request(
                request,
                FakeResponse(b""),
                302,
                "Found",
                {},
                "https://release-assets.githubusercontent.com/asset.zip",
            )
            self.assertIsNotNone(redirected)
            self.assertNotIn("Authorization", redirected.headers)
            self.assertNotIn("Authorization", redirected.unredirected_hdrs)
            self.assertEqual(target.read_bytes(), content)

    def test_release_download_url_does_not_receive_github_api_token(self) -> None:
        content = b"direct release bytes"
        expected = hashlib.sha256(content).hexdigest()
        with tempfile.TemporaryDirectory() as temporary_directory:
            target = Path(temporary_directory) / "asset.exe"
            with patch(
                "scripts.fetch_dependencies.urllib.request.urlopen",
                return_value=FakeResponse(content),
            ) as open_url, patch.dict("os.environ", {"GH_TOKEN": "test-token"}):
                fetch(
                    "https://github.com/vendor/project/releases/download/v1/asset.exe",
                    target,
                    expected,
                )
            request = open_url.call_args.args[0]
            self.assertNotIn("Authorization", request.unredirected_hdrs)
            self.assertIsNone(request.get_header("Authorization"))

    def test_extracts_only_the_expected_deno_executable(self) -> None:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        component = next(item for item in manifest["components"] if item["id"] == "deno")
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            archive_path = root / "deno.zip"
            output = root / "out"
            output.mkdir()
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("deno-x86_64/deno.exe", b"runtime")
                archive.writestr("deno-x86_64/LICENSE.txt", b"license")
            extract_component(component, archive_path, output)
            self.assertEqual((output / "deno.exe").read_bytes(), b"runtime")
            self.assertEqual(
                (output / "licenses" / "deno" / "archive" / "deno-x86_64" / "LICENSE.txt").read_bytes(),
                b"license",
            )
            copied_notices = {
                notice["destination"]: (output / "licenses" / "deno" / notice["destination"]).read_bytes()
                for notice in component["license_files"]
            }
            self.assertIn(b"MIT License", copied_notices["Deno-LICENSE.md"])
            self.assertIn(
                b"Apache License", copied_notices["TypeScript-Apache-2.0-LICENSE.txt"]
            )
            self.assertIn(b"Microsoft Corporation", copied_notices["Node-types-MIT-LICENSE.txt"])
            self.assertIn(b"Undici contributors", copied_notices["Undici-MIT-LICENSE.txt"])
            self.assertIn(
                b"does not provide one consolidated notice bundle",
                copied_notices["UPSTREAM-NOTICES.md"],
            )

    def test_extracts_ffmpeg_runtime_dlls_and_archive_license(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            archive_path = root / "ffmpeg.zip"
            output = root / "out"
            output.mkdir()
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("ffmpeg/bin/ffmpeg.exe", b"ffmpeg")
                archive.writestr("ffmpeg/bin/ffprobe.exe", b"ffprobe")
                archive.writestr("ffmpeg/bin/avcodec-62.dll", b"runtime dll")
                archive.writestr("ffmpeg/bin/ffplay.exe", b"unused executable")
                archive.writestr("ffmpeg/LICENSE.txt", b"LGPL v3 license")
            extract_component({"extract": "ffmpeg"}, archive_path, output)
            self.assertEqual((output / "ffmpeg.exe").read_bytes(), b"ffmpeg")
            self.assertEqual((output / "ffprobe.exe").read_bytes(), b"ffprobe")
            self.assertEqual((output / "avcodec-62.dll").read_bytes(), b"runtime dll")
            self.assertFalse((output / "ffplay.exe").exists())
            self.assertEqual(
                (output / "licenses" / "ffmpeg" / "LICENSE.txt").read_bytes(),
                b"LGPL v3 license",
            )

    def test_manifest_declares_runtime_dependencies_and_verified_ffmpeg_provenance(self) -> None:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        validate_manifest(manifest)
        self.assertTrue(
            all(component["runtime_dependencies"] for component in manifest["components"])
        )
        ffmpeg = next(item for item in manifest["components"] if item["id"] == "ffmpeg")
        self.assertEqual(
            ffmpeg["url"],
            "https://api.github.com/repos/BtbN/FFmpeg-Builds/releases/assets/659413383",
        )
        self.assertEqual(
            ffmpeg["sha256"],
            "3a584c6017cd9c43716d13bb244e77cd7c89a71e1395bb244f7751a405e3b3fc",
        )
        self.assertIn("n8.1.3-18-g46d899d7a0-20261020", ffmpeg["version"])
        self.assertIn("LGPL-3.0-or-later", ffmpeg["license"])
        self.assertNotIn("LGPL-2.1", ffmpeg["license"])

    def test_manifest_rejects_a_component_without_runtime_dependencies(self) -> None:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        manifest["components"][0].pop("runtime_dependencies")
        with self.assertRaisesRegex(ValueError, "runtime_dependencies"):
            validate_manifest(manifest)


if __name__ == "__main__":
    unittest.main()
