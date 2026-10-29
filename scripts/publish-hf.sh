#!/usr/bin/env bash
# Publish a completed run, including its generated card and upstream data terms.
# Tokens come from HF_TOKEN or huggingface_hub's cached login, never command arguments.
set -eu
OUT="${1:?usage: publish-hf.sh <run-out-dir> [repo-id]}"
REPO="${2:-SkyeAv/amalgam}"
exec uvx --from huggingface_hub --with pyyaml python "$(dirname "$0")/publish_hf.py" "$OUT" "$REPO"
