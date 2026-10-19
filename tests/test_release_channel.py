from __future__ import annotations

import unittest

from scripts.release_channel import classify_release_tag


class ReleaseChannelTests(unittest.TestCase):
    def test_plain_semver_tags_are_stable(self) -> None:
        self.assertEqual(
            classify_release_tag("v1.2.3"),
            {"version": "1.2.3", "channel": "stable"},
        )

    def test_semver_prerelease_tags_are_preview(self) -> None:
        for tag in ("v1.2.3-preview.1", "v0.9.0-rc.2", "v1.2.3-alpha+build.4"):
            with self.subTest(tag=tag):
                result = classify_release_tag(tag)
                self.assertEqual(result["channel"], "preview")
                self.assertEqual(result["version"], tag.removeprefix("v"))

    def test_build_metadata_does_not_turn_a_stable_tag_into_preview(self) -> None:
        self.assertEqual(
            classify_release_tag("v1.2.3+build.4"),
            {"version": "1.2.3+build.4", "channel": "stable"},
        )

    def test_invalid_or_non_semver_tags_are_rejected(self) -> None:
        for tag in (
            "1.2.3",
            "v1.2",
            "v01.2.3",
            "v1.2.3-",
            "v1.2.3-preview..1",
            "v1.2.3-01",
            "v1.2.3+",
        ):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                classify_release_tag(tag)


if __name__ == "__main__":
    unittest.main()
