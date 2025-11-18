#!/usr/bin/env bash
set -Eeuo pipefail

# apply-microceph-osd-plan.sh
#
# Apply a MicroCeph OSD plan produced by plan-microceph-osds.sh.
#
# - Reads a CSV:
#     osd_id,data_dev,db_dev,wal_dev,combined_dbwal
# - For each row:
#     * Checks that data_dev exists.
#     * Skips if MicroCeph already uses that disk.
#     * Prints (dry-run) or runs: microceph disk add <data_dev>
#
# - Safe by default: DRY-RUN unless --apply is given.
#
# Usage:
#   sudo ./apply-microceph-osd-plan.sh --plan /tmp/microceph-osd-plan.ABC123.csv
#   sudo ./apply-microceph-osd-plan.sh --plan plan.csv --apply
#
# Optional filters:
#   --start-osd N    # only apply from osdN ...
#   --end-osd M      # ... up to osdM (inclusive)

die(){ echo -e "\e[31m[FATAL]\e[0m $*" >&2; exit 1; }
warn(){ echo -e "\e[33m[WARN]\e[0m  $*" >&2; }
info(){ echo "[INFO] $*"; }
ok(){ echo -e "\e[32m[OK]\e[0m    $*"; }
dry(){ echo -e "\e[36m[DRY]\e[0m   $*"; }

APPLY=false
PLAN=""
START_OSD=""
END_OSD=""

usage() {
  cat <<EOF
Usage: $0 --plan /path/to/microceph-osd-plan.csv [--apply] [--start-osd N] [--end-osd M]

Options:
  --plan FILE       CSV produced by plan-microceph-osds.sh (required)
  --apply           Actually run 'microceph disk add'; without this, dry-run only
  --start-osd N     Only apply for OSDs with numeric index >= N (e.g. osd5 => 5)
  --end-osd M       Only apply for OSDs with numeric index <= M
  -h, --help        Show this help
EOF
}

# -------- arg parsing --------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --plan) PLAN="${2:-}"; shift ;;
    --apply) APPLY=true ;;
    --start-osd) START_OSD="${2:-}"; shift ;;
    --end-osd) END_OSD="${2:-}"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown option: $1" ;;
  esac
  shift
done

[[ -n "$PLAN" ]] || die "--plan FILE is required"
[[ -f "$PLAN" ]] || die "Plan file not found: $PLAN"

if [[ -n "$START_OSD" && ! "$START_OSD" =~ ^[0-9]+$ ]]; then
  die "--start-osd must be a positive integer"
fi
if [[ -n "$END_OSD" && ! "$END_OSD" =~ ^[0-9]+$ ]]; then
  die "--end-osd must be a positive integer"
fi

command -v microceph >/dev/null 2>&1 || die "microceph command not found in PATH"
command -v readlink  >/dev/null 2>&1 || die "readlink command not found in PATH"

info "Plan file: $PLAN"
$APPLY || warn "Running in DRY-RUN mode (no changes will be made). Use --apply to actually add disks."

# -------- detect current MicroCeph disks --------

declare -A USED_REAL

command -v jq >/dev/null 2>&1 || die "jq is required but not installed (used to parse 'microceph disk list --json')"

info "Querying existing MicroCeph disks via 'microceph disk list --json --host-only'..."

json="$(microceph disk list --json --host-only 2>/dev/null || true)"

if [[ -z "$json" ]]; then
  warn "microceph disk list --json returned no data; assuming no disks are configured yet."
else
  # Extract only ConfiguredDisks[*].path and resolve to the real device node
  while IFS= read -r cfg_path; do
    [[ -z "$cfg_path" ]] && continue

    # Resolve symlink (e.g. /dev/disk/by-mfast/meta1 -> /dev/nvme0n1)
    real="$(readlink -f "$cfg_path" 2>/dev/null || echo "$cfg_path")"

    USED_REAL["$real"]=1
  done < <(jq -r '.ConfiguredDisks[]?.path // empty' <<<"$json")
fi

info "Detected ${#USED_REAL[@]} disk(s) already in use by MicroCeph."


# -------- process plan --------

echo
info "Processing plan..."

# Track how many we actually acted on
added_count=0
skipped_existing=0
skipped_filtered=0
skipped_missing=0

# Read CSV, skip header
{
  read -r header || true  # skip header line
  while IFS=, read -r osd_id data_dev db_dev wal_dev combined; do
    # Skip empty lines
    [[ -z "$osd_id" ]] && continue

    # Extract numeric index from osd_id like "osd5"
    if [[ "$osd_id" =~ ^osd([0-9]+)$ ]]; then
      idx="${BASH_REMATCH[1]}"
    else
      warn "Skipping row with unexpected osd_id format: '$osd_id'"
      continue
    fi

    # Range filtering
    if [[ -n "$START_OSD" && "$idx" -lt "$START_OSD" ]]; then
      ((skipped_filtered++))
      continue
    fi
    if [[ -n "$END_OSD" && "$idx" -gt "$END_OSD" ]]; then
      ((skipped_filtered++))
      continue
    fi

    # data_dev should be something like /dev/disk/by-id/wwn-...
    if [[ -z "$data_dev" ]]; then
      warn "osd${idx}: no data_dev in plan; skipping"
      ((skipped_missing++))
      continue
    fi

    if [[ ! -e "$data_dev" ]]; then
      warn "osd${idx}: data_dev '$data_dev' does not exist; skipping"
      ((skipped_missing++))
      continue
    fi

    real_data="$(readlink -f "$data_dev" 2>/dev/null || echo "$data_dev")"

    # If MicroCeph already owns this disk, skip
    if [[ -n "${USED_REAL[$real_data]:-}" ]]; then
      info "osd${idx}: $data_dev (real=$real_data) already used by MicroCeph; skipping"
      ((skipped_existing++))
      continue
    fi

    # Compose informative comment for log/dry-run
    comment_parts=()
    [[ -n "$db_dev"  ]] && comment_parts+=("db=$db_dev")
    [[ -n "$wal_dev" ]] && comment_parts+=("wal=$wal_dev")
    [[ -n "$combined" ]] && comment_parts+=("combined_dbwal=$combined")

    comment=""
    if ((${#comment_parts[@]} > 0)); then
      comment="# ${comment_parts[*]}"
    fi

    if $APPLY; then
      info "osd${idx}: adding $data_dev via 'microceph disk add' $comment"
      microceph disk add "$data_dev"
      ok   "osd${idx}: microceph disk add $data_dev completed"
      ((added_count++))
      # Mark it as used so subsequent rows can't try to reuse it
      USED_REAL["$real_data"]=1
    else
      dry "osd${idx}: microceph disk add \"$data_dev\"  $comment"
      ((added_count++))
    fi

  done
} < "$PLAN"

echo
info "Summary:"
echo "  Planned rows processed:   $added_count"
echo "  Skipped (already used):  $skipped_existing"
echo "  Skipped (filtered):      $skipped_filtered"
echo "  Skipped (missing data):  $skipped_missing"

if $APPLY; then
  ok "Finished applying MicroCeph OSD plan."
else
  warn "This was a DRY-RUN. Re-run with --apply to actually add disks."
fi
