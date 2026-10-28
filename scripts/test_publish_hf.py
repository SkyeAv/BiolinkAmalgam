"""Offline upload contract tests: no credentials and no Hub calls."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from publish_hf import publish, upload_files


class PublishTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.out = Path(self.temp.name)
        self.card = """---
license: other
license_link: LICENSE.md
configs:
  - config_name: cliques
    default: true
    data_files:
      - split: gene
        path: cliques/gene.parquet
  - config_name: crossrefs
    data_files:
      - split: clinvar
        path: crossrefs/clinvar.parquet
---
# Dataset
"""
        self.write("README.md", self.card)
        for f in ["LICENSE.md", "cliques/gene.parquet", "crossrefs/clinvar.parquet"]:
            self.write(f, "fixture")

    def write(self, name, content):
        p = self.out / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)

    def test_exact_files_only_and_migration_deletes(self):
        for name in ["crossrefs/stale.parquet", "crossrefs/crash.parts/part-0.parquet", "cliques.parts/part-0.parquet"]:
            self.write(name, "debris")
        api = Mock()
        publish(self.out, "test/repo", api)
        call = api.upload_folder.call_args.kwargs
        self.assertEqual(call["allow_patterns"], ["README.md", "LICENSE.md", "cliques/gene.parquet", "crossrefs/clinvar.parquet"])
        self.assertIn("biolinkamalgam.parquet", call["delete_patterns"])
        self.assertIn("crossrefs/*.parquet", call["delete_patterns"])

    def test_missing_file_fails_before_mutation(self):
        (self.out / "crossrefs/clinvar.parquet").unlink()
        api = Mock()
        with self.assertRaises(ValueError):
            publish(self.out, "test/repo", api)
        api.create_repo.assert_not_called()
        api.upload_folder.assert_not_called()

    def test_legacy_card_rejected(self):
        self.write("README.md", self.card.replace("config_name: cliques", "config_name: default"))
        with self.assertRaises(ValueError):
            upload_files(self.out)

    def test_unsafe_path_and_duplicate_split_rejected(self):
        for card in [
            self.card.replace("path: cliques/gene.parquet", "path: ../secret"),
            self.card.replace("path: cliques/gene.parquet", "path: cliques/*.parquet"),
            self.card.replace("split: gene", "split: ../gene"),
            self.card.replace("  - config_name: crossrefs", "      - split: gene\n        path: cliques/gene.parquet\n  - config_name: crossrefs"),
        ]:
            self.write("README.md", card)
            with self.assertRaises(ValueError):
                upload_files(self.out)


if __name__ == "__main__":
    unittest.main()
