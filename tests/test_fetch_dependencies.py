from __future__ import annotations

import hashlib
import io
import tempfile
import unittest
import zipfile
from pathlib import Path
from urllib.request import HTTPRedirectHandler
from unittest.mock import patch

from scripts.fetch_dependencies import extract_component, fetch, safe_name


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
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            archive_path = root / "deno.zip"
            output = root / "out"
            output.mkdir()
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("deno-x86_64/deno.exe", b"runtime")
                archive.writestr("deno-x86_64/LICENSE.txt", b"license")
            extract_component({"extract": "deno", "asset": "deno.zip"}, archive_path, output)
            self.assertEqual((output / "deno.exe").read_bytes(), b"runtime")
            self.assertEqual((output / "licenses" / "deno" / "LICENSE.txt").read_bytes(), b"license")


if __name__ == "__main__":
    unittest.main()
