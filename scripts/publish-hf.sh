#!/usr/bin/env bash
# publish-hf: create (if absent) the PUBLIC Hugging Face dataset repo and upload
# a finished run's parquet + generated dataset card.
#
#   scripts/publish-hf.sh <run-out-dir> [repo-id]
#
# repo-id defaults to SkyeAv/babel-amalgam. The token is read from $HF_TOKEN
# (never passed on the command line, never logged). Only uploads the
# deliverable + card: audit TSVs and the manifest stay local.
set -eu

OUT="${1:?usage: publish-hf.sh <run-out-dir> [repo-id]}"
REPO="${2:-SkyeAv/babel-amalgam}"

: "${HF_TOKEN:?HF_TOKEN must be set in the environment}"
[ -f "$OUT/biolinkamalgam.parquet" ] || { echo "missing $OUT/biolinkamalgam.parquet" >&2; exit 1; }
[ -f "$OUT/README.md" ] || { echo "missing $OUT/README.md (dataset card)" >&2; exit 1; }

# huggingface_hub via uvx: no global install. Python reads the token from env.
uvx --from huggingface_hub python - "$OUT" "$REPO" <<'PY'
import os, sys
from huggingface_hub import HfApi

out, repo = sys.argv[1], sys.argv[2]
api = HfApi(token=os.environ["HF_TOKEN"])
api.create_repo(repo, repo_type="dataset", private=False, exist_ok=True)
api.upload_folder(
    folder_path=out,
    repo_id=repo,
    repo_type="dataset",
    allow_patterns=["biolinkamalgam.parquet", "README.md"],
    commit_message="Publish BiolinkAmalgam build",
)
print(f"published https://huggingface.co/datasets/{repo}")
PY
