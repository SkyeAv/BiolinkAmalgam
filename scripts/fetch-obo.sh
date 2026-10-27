#!/usr/bin/env bash
# fetch-obo: download every ontology in config/obo_sources.yaml into <dir> as <prefix-lowercase>.obo.
# Resumable (aria2c --continue); fails if any file is missing or is not an OBO document.
set -euo pipefail
DIR="${1:?usage: fetch-obo.sh <dir> [config]}"
CFG="${2:-config/obo_sources.yaml}"
mkdir -p "$DIR"
list=$(mktemp)
trap 'rm -f "$list"' EXIT
awk '/- prefix:/{p=tolower($3)} /url:/{print $2"\n  out="p".obo"}' "$CFG" > "$list"
[ -s "$list" ] || { echo "no ontologies in $CFG" >&2; exit 1; }
aria2c --continue=true --max-concurrent-downloads=8 --max-connection-per-server=4 \
  --allow-overwrite=false --auto-file-renaming=false --dir="$DIR" --input-file="$list"
awk '/- prefix:/{print tolower($3)}' "$CFG" | while read -r p; do
  f="$DIR/$p.obo"
  [ -s "$f" ] || { echo "missing $f" >&2; exit 1; }
  head -c 4096 "$f" | grep -q '^format-version:' || { echo "not OBO: $f" >&2; exit 1; }
done
echo "fetched $(ls "$DIR"/*.obo | wc -l) OBO files into $DIR"
