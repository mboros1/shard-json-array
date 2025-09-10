#!/usr/bin/env bash
set -euo pipefail

# Tiny end-to-end smoke test:
# - Generates a small JSON dataset
# - Shards it into zstd NDJSON files
# - Verifies counts match and that shard files contain only valid JSON lines (no garbage)
#
# Requirements: cargo, jq, zstd
#
# Usage:
#   scripts/e2e_smoke.sh [N_ENTITIES] [N_SHARDS]
#
# Defaults:
#   N_ENTITIES=2000, N_SHARDS=4

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo not found; install Rust toolchain" >&2
  exit 127
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "error: jq not found; please install jq" >&2
  exit 127
fi
if ! command -v zstd >/dev/null 2>&1; then
  echo "error: zstd not found; please install zstd" >&2
  exit 127
fi

N_ENTITIES=${1:-2000}
N_SHARDS=${2:-4}

tmpdir=$(mktemp -d 2>/dev/null || mktemp -d -t e2e)
trap 'rm -rf "$tmpdir"' EXIT

echo "Temp dir: $tmpdir"

set -x
cargo build --release

# Generate test data: object with {"entities": [...]}
target/release/generate-entities "$N_ENTITIES" -o "$tmpdir/entities.json"

# Shard into N_SHARDS
target/release/shard-json-array "$tmpdir/entities.json" "$N_SHARDS" --array-key entities

# Check counts and basic integrity
scripts/check_shards.sh "$tmpdir/entities.json" --array-key entities --glob "$tmpdir/entities.shard-*.ndjson.zst"

# Extra safety checks on each shard: compressed integrity, newline at EOF, no blank lines,
# and jq fully parses each line (one JSON per line with no trailing junk).
for f in "$tmpdir"/entities.shard-*.ndjson.zst; do
  # Compressed integrity
  zstd -t -- "$f"

  # No blank lines; newline at EOF
  raw="$tmpdir/raw.txt"
  zstd -dc -- "$f" > "$raw"
  if grep -n '^[[:space:]]*$' "$raw" >/dev/null; then
    echo "ERROR: blank line(s) found in $(basename "$f")" >&2
    exit 4
  fi
  last=$(tail -c 1 "$raw" | od -An -t u1 | tr -d ' \n')
  if [[ "$last" != "10" ]]; then
    echo "ERROR: $(basename "$f") does not end with a newline" >&2
    exit 5
  fi

  # jq parses every line; counts match
  lines=$(wc -l < "$raw" | awk '{print $1+0}')
  parsed=$(jq -c . < "$raw" | wc -l | awk '{print $1+0}')
  if [[ "$lines" -ne "$parsed" ]]; then
    echo "ERROR: parsed line count ($parsed) != raw line count ($lines) in $(basename "$f")" >&2
    exit 6
  fi
done
set +x

echo "OK: end-to-end smoke test passed (entities=$N_ENTITIES, shards=$N_SHARDS)"
