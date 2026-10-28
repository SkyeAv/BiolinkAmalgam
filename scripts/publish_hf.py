"""Publish only card-declared, complete dataset files. Importable for offline tests."""

import re
from pathlib import Path

import yaml


def upload_files(out):
    """Fail before any Hub mutation if the card or the declared deliverables are incomplete."""
    out = Path(out)
    card = (out / "README.md").read_text()
    if not card.startswith("---\n") or "\n---\n" not in card[4:]:
        raise ValueError("README.md must contain YAML frontmatter")
    metadata = yaml.safe_load(card.split("---\n", 2)[1])
    configs = metadata.get("configs", [])
    if sorted(c.get("config_name", "") for c in configs) != ["cliques", "crossrefs"]:
        raise ValueError("expected exactly the cliques and crossrefs subsets")
    if metadata.get("license_link") != "LICENSE.md":
        raise ValueError("card must link LICENSE.md")
    files = ["README.md", "LICENSE.md"]
    for config in configs:
        subset = config["config_name"]
        splits = config.get("data_files", [])
        if not splits:
            raise ValueError(f"{subset} has no splits")
        seen = set()
        for entry in splits:
            split = entry["split"]
            if not isinstance(split, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", split):
                raise ValueError(f"invalid split: {split!r}")
            if split in seen:
                raise ValueError(f"duplicate {subset} split: {split}")
            seen.add(split)
            expected = f"{subset}/{split}.parquet"
            if entry["path"] != expected:
                raise ValueError(f"expected exact path {expected}")
            files.append(expected)
    for name in files:
        path = out / name
        if not path.is_file() or path.is_symlink() or path.stat().st_size == 0:
            raise ValueError(f"missing, empty or symlinked deliverable: {path}")
    return files


def publish(out, repo, api):
    files = upload_files(out)
    api.create_repo(repo, repo_type="dataset", private=False, exist_ok=True)
    api.upload_folder(
        folder_path=str(out),
        repo_id=repo,
        repo_type="dataset",
        allow_patterns=files,
        # Replace obsolete data in the same commit; unrelated Hub files are untouched.
        delete_patterns=["biolinkamalgam.parquet", "crossrefs.parquet", "cliques/*.parquet", "crossrefs/*.parquet"],
        commit_message="Publish BiolinkAmalgam build",
    )


def main():
    import os
    import sys

    from huggingface_hub import HfApi

    out, repo = sys.argv[1:]
    # Validate before authentication or any network activity.
    upload_files(out)
    api = HfApi(token=os.environ.get("HF_TOKEN") or None)
    api.whoami()
    publish(out, repo, api)
    print(f"published https://huggingface.co/datasets/{repo}")


if __name__ == "__main__":
    main()
