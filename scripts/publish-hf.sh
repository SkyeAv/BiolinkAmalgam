#!/usr/bin/env bash
# publish-hf: create (if absent) the PUBLIC Hugging Face dataset repo and upload
# a finished run's parquet + generated dataset card.
#
#   scripts/publish-hf.sh <run-out-dir> [repo-id]
#
# repo-id defaults to SkyeAv/babel-amalgam. The token comes from $HF_TOKEN or,
# failing that, the standard Hugging Face token file (~/.cache/huggingface/token;
# huggingface_hub reads it when no token is passed). Never on the command line,
# never logged. Only uploads the deliverable + card: audit TSVs and the manifest
# stay local.
set -eu

OUT="${1:?usage: publish-hf.sh <run-out-dir> [repo-id]}"
REPO="${2:-SkyeAv/babel-amalgam}"

if [ -z "${HF_TOKEN:-}" ] && [ ! -f "$HOME/.cache/huggingface/token" ]; then
  echo "no HF_TOKEN and no ~/.cache/huggingface/token" >&2
  exit 1
fi
[ -f "$OUT/biolinkamalgam.parquet" ] || { echo "missing $OUT/biolinkamalgam.parquet" >&2; exit 1; }
[ -f "$OUT/README.md" ] || { echo "missing $OUT/README.md (dataset card)" >&2; exit 1; }

# huggingface_hub via uvx: no global install. Python reads the token from env.
uvx --from huggingface_hub python - "$OUT" "$REPO" <<'PY'
import os, sys
from huggingface_hub import HfApi

out, repo = sys.argv[1], sys.argv[2]
# Prefer $HF_TOKEN; if it is set but stale (401), fall back to the cached token
# file. Whichever source wins is named, secrets are never printed.
def make_api():
    tok = os.environ.get("HF_TOKEN")
    if tok:
        api = HfApi(token=tok)
        try:
            api.whoami()
            print("auth: HF_TOKEN")
            return api
        except Exception as e:
            print(f"auth: HF_TOKEN rejected ({str(e)[:60]}); falling back to token file")
    # HfApi() would pick up the stale HF_TOKEN env again; read the file explicitly
    tok_file = os.path.expanduser("~/.cache/huggingface/token")
    with open(tok_file) as f:
        api = HfApi(token=f.read().strip())
    api.whoami()  # fail loudly here if neither works
    print("auth: token file")
    return api

api = make_api()
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
