#!/usr/bin/env bash
# validate-run: pre-publication sanity checks on a finished run.
# Usage: scripts/validate-run.sh <run-out-dir> [extra args to pass through]
set -eu
OUT="${1:?usage: validate-run.sh <run-out-dir>}"
shift || true
cd "$(dirname "$0")/.."
JAR=out/biolinkamalgam/assembly.dest/out.jar
[ -f "$JAR" ] || { echo "missing $JAR — run: ./mill biolinkamalgam.assembly" >&2; exit 1; }
exec java ${JAVA_OPTS:-} -jar "$JAR" validate --out "$OUT" "$@"
