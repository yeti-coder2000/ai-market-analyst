from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.services.research.v3.workspace import (
    WorkspaceSafetyError,
    assert_safe_output_root,
    deterministic_tree_hash,
    exact_write_probe,
    path_is_within,
)


class ResearchV3WorkspaceTests(unittest.TestCase):
    def test_overlap_and_symlink_alias_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            protected = base / "protected"
            protected.mkdir()
            with self.assertRaises(WorkspaceSafetyError):
                assert_safe_output_root(protected / "child", [protected])
            with self.assertRaises(WorkspaceSafetyError):
                assert_safe_output_root(base, [protected])
            alias = base / "alias"
            alias.symlink_to(protected, target_is_directory=True)
            with self.assertRaises(WorkspaceSafetyError):
                assert_safe_output_root(alias, [protected])

    def test_exact_write_probe_removes_only_probe(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            protected = base / "protected"
            protected.mkdir()
            output = base / "output"
            output.mkdir()
            sentinel = output / "sentinel.txt"
            sentinel.write_text("preserve", encoding="utf-8")
            result = exact_write_probe(output, [protected])
            self.assertTrue(result["passed"])
            self.assertEqual(
                sentinel.read_text(encoding="utf-8"), "preserve"
            )
            self.assertEqual(
                [path.name for path in output.iterdir()], ["sentinel.txt"]
            )

    def test_tree_hash_is_deterministic_and_content_sensitive(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            (base / "b").mkdir()
            (base / "b" / "two.txt").write_text("two", encoding="utf-8")
            (base / "one.txt").write_text("one", encoding="utf-8")
            first = deterministic_tree_hash(base)
            second = deterministic_tree_hash(base)
            self.assertEqual(first, second)
            (base / "one.txt").write_text("changed", encoding="utf-8")
            self.assertNotEqual(
                first["sha256"], deterministic_tree_hash(base)["sha256"]
            )

    def test_path_is_within_uses_canonical_identity(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            child = base / "a" / "b"
            child.mkdir(parents=True)
            self.assertTrue(path_is_within(child, base))
            self.assertFalse(path_is_within(base, child))


if __name__ == "__main__":
    unittest.main()
