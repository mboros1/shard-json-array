#!/usr/bin/env bash
set -euo pipefail

# Verify integrity between a large JSON input and its zstd-compressed NDJSON shards.
#
# - Counts input entities in a streaming way via jq (no full materialization).
# - Validates every shard line is valid JSON and counts lines.
# - Compares total counts; optionally verifies ID set equality.
#
# Requirements: jq, zstd
#
# Usage:
#   scripts/check_shards.sh <input.json> [--array-key KEY] [--glob GLOB] [--check-ids] [--ids-field FIELD]
#
# Examples:
#   scripts/check_shards.sh array.json
#   scripts/check_shards.sh entities.json --array-key entities
#   scripts/check_shards.sh entities.json --array-key entities --check-ids --ids-field id
#   scripts/check_shards.sh entities.json --array-key entities --glob 'data/entities.shard-*.ndjson.zst'

if ! command -v jq >/dev/null 2>&1; then
  echo "error: jq not found; please install jq" >&2
  exit 127
fi
if ! command -v zstd >/dev/null 2>&1; then
  echo "error: zstd not found; please install zstd" >&2
  exit 127
fi

ARRAY_KEY=""
CHECK_IDS=0
IDS_FIELD="id"
GLOB_OVERRIDE=""

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <input.json> [--array-key KEY] [--glob GLOB] [--check-ids] [--ids-field FIELD]" >&2
  exit 2
fi

INPUT="${1}"
shift || true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --array-key)
      ARRAY_KEY="${2:-}"
      shift 2 || true
      ;;
    --check-ids)
      CHECK_IDS=1
      shift || true
      ;;
    --ids-field)
      IDS_FIELD="${2:-id}"
      shift 2 || true
      ;;
    --glob)
      GLOB_OVERRIDE="${2:-}"
      shift 2 || true
      ;;
    -h|--help)
      echo "usage: $0 <input.json> [--array-key KEY] [--glob GLOB] [--check-ids] [--ids-field FIELD]" >&2
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ ! -f "$INPUT" ]]; then
  echo "error: input file not found: $INPUT" >&2
  exit 1
fi

dir="$(cd "$(dirname "$INPUT")" && pwd)"
base="$(basename "$INPUT")"
stem="${base%.*}"

shard_glob="${GLOB_OVERRIDE:-$dir/${stem}.shard-*-of-*.ndjson.zst}"
# Expand the glob robustly; when no files match, produce an empty array (no empty string entries).
mapfile -t SHARDS < <(compgen -G "$shard_glob" || true)

if [[ ${#SHARDS[@]} -eq 0 ]]; then
  echo "error: no shard files matched glob: $shard_glob" >&2
  exit 1
fi

echo "Input:    $INPUT"
echo "Shards:   ${#SHARDS[@]} files (glob: $shard_glob)"
echo "ArrayKey: ${ARRAY_KEY:-<top-level array>}"

echo "Counting input entities (streaming via jq)..." >&2
if [[ -z "$ARRAY_KEY" ]]; then
  # Count top-level array elements in a streaming manner.
  input_count=$(jq -c 'if type=="array" then .[] else error("expected top-level array; use --array-key") end | 1' "$INPUT" | wc -l | awk '{print $1+0}')
else
  input_count=$(jq -c --arg k "$ARRAY_KEY" '.[$k][] | 1' "$INPUT" | wc -l | awk '{print $1+0}')
fi
echo "Input count: $input_count"

echo "Validating shards and counting lines..." >&2
total_out=0
for f in "${SHARDS[@]}"; do
  c=$(zstd -dc -- "$f" | jq -c . | awk 'END{print NR+0}')
  echo "  $(basename "$f"): $c"
  total_out=$((total_out + c))
done
echo "Shards total: $total_out"

if [[ "$total_out" -ne "$input_count" ]]; then
  echo "ERROR: mismatch: input ($input_count) != shards ($total_out)" >&2
  exit 3
fi

if [[ "$CHECK_IDS" -eq 1 ]]; then
  echo "Checking ID set equality (field: $IDS_FIELD)..." >&2
  tmpdir=$(mktemp -d)
  trap 'rm -rf "$tmpdir"' EXIT
  in_ids="$tmpdir/in_ids.txt"
  out_ids="$tmpdir/out_ids.txt"

  if [[ -z "$ARRAY_KEY" ]]; then
    jq -r --arg f "$IDS_FIELD" 'if type=="array" then .[] else error("expected top-level array; use --array-key") end | .[$f] // empty' "$INPUT" > "$in_ids"
  else
    jq -r --arg k "$ARRAY_KEY" --arg f "$IDS_FIELD" '.[$k][] | .[$f] // empty' "$INPUT" > "$in_ids"
  fi

  for f in "${SHARDS[@]}"; do
    zstd -dc -- "$f" | jq -r --arg f "$IDS_FIELD" '.[$f] // empty' >> "$out_ids"
  done

  # Sort and compare
  LC_ALL=C sort -u "$in_ids" -o "$in_ids.sorted"
  LC_ALL=C sort -u "$out_ids" -o "$out_ids.sorted"

  in_n=$(wc -l < "$in_ids.sorted" | awk '{print $1+0}')
  out_n=$(wc -l < "$out_ids.sorted" | awk '{print $1+0}')
  echo "Unique IDs: input=$in_n, shards=$out_n"

  if ! diff -q "$in_ids.sorted" "$out_ids.sorted" >/dev/null; then
    echo "ERROR: ID sets differ between input and shards" >&2
    echo "First few differences (input vs shards):" >&2
    comm -3 "$in_ids.sorted" "$out_ids.sorted" | head -n 50 >&2
    exit 4
  fi

  # Also check for duplicates within the shards aggregate (optional)
  dups=$(LC_ALL=C sort "$out_ids" | uniq -d | head -n 1 || true)
  if [[ -n "$dups" ]]; then
    echo "ERROR: duplicate ID detected in shards: $dups" >&2
    exit 5
  fi
fi

echo "OK: counts match and all shard lines are valid JSON." >&2
if [[ "$CHECK_IDS" -eq 1 ]]; then
  echo "OK: ID sets match with no duplicates." >&2
fi
