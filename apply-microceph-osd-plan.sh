#!/usr/bin/env bash
set -Eeuo pipefail

# apply-microceph-osd-plan.sh
#   Apply a microceph OSD plan CSV produced by the planner script.
#
# CSV format (header required):
#   osd,rotating,db,wal,combined_dbwal
#   osd1,/dev/disk/by-id/wwn-...,/dev/disk/by-mfast/osd1-db,/dev/disk/by-mfast/osd1-wal,no
#
# Behaviour:
#   - Uses `microceph disk list --json --host-only` + jq to discover *configured* disks.
#   - Skips any rotating disk already used by MicroCeph (by path and by real device).
#   - Builds `microceph disk add` commands with appropriate DB/WAL options.
#   - Dry-run by default; use --apply to actually run commands.
#
# Requirements:
#   - microceph
#   - jq
#   - readlink, awk, sed

APPLY=false
PLAN_FILE=""
START_OSD=1
END_OSD=999999

die(){ echo -e "\e[31m[FATAL]\e[0m $*" >&2; exit 1; }
warn(){ echo -e "\e[33m[WARN]\e[0m  $*" >&2; }
info(){ echo -e "[INFO] $*"; }
dry(){ echo -e "\e[36m[DRY]\e[0m   $*"; }

usage() {
  cat <<EOF
Usage: $0 --plan /path/to/microceph-osd-plan.csv [options]

Options:
  --plan PATH        Plan CSV file to apply (required)
  --apply            Actually run microceph disk add (default: dry-run)
  --start-osd N      First OSD index to apply (default: 1)
  --end-osd M        Last OSD index to apply (default: very large)
  -h, --help         Show this help
EOF
}

need_cmds=(microceph jq readlink awk sed)
for c in "${need_cmds[@]}"; do
  command -v "$c" >/dev/null || die "Missing required command: $c"
done

# ---------------- arg parsing ----------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --plan) PLAN_FILE="${2:?}"; shift ;;
    --apply) APPLY=true ;;
    --start-osd) START_OSD="${2:?}"; shift ;;
    --end-osd) END_OSD="${2:?}"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown option: $1" ;;
  esac
  shift
done

[[ -n "$PLAN_FILE" ]] || die "You must specify --plan /path/to/plan.csv"
[[ -f "$PLAN_FILE" ]] || die "Plan file not found: $PLAN_FILE"

[[ "$START_OSD" =~ ^[0-9]+$ ]] || die "--start-osd must be an integer"
[[ "$END_OSD" =~ ^[0-9]+$ ]] || die "--end-osd must be an integer"

echo "[INFO] Plan file: $PLAN_FILE"
if ! $APPLY; then
  warn "Running in DRY-RUN mode (no changes will be made). Use --apply to actually add disks."
fi

# ---------------- discover existing MicroCeph disks ----------------
info "Querying existing MicroCeph disks via 'microceph disk list --json --host-only'..."

json_out="$(microceph disk list --json --host-only 2>/dev/null || echo "")"

declare -a CONFIGURED_PATHS=()
if [[ -n "$json_out" ]]; then
  # Only ConfiguredDisks[].Path are "in use", everything in AvailableDisks is *not* configured yet.
  while IFS= read -r p; do
    [[ -n "$p" ]] && CONFIGURED_PATHS+=("$p")
  done < <(printf '%s\n' "$json_out" | jq -r '.ConfiguredDisks[].Path // empty')
fi

configured_count=${#CONFIGURED_PATHS[@]}
info "Detected ${configured_count} disk(s) already in use by MicroCeph."

# Build lookup maps by path and by real device
declare -A USED_PATH USED_REAL
for p in "${CONFIGURED_PATHS[@]}"; do
  USED_PATH["$p"]=1
  real="$(readlink -f "$p" 2>/dev/null || echo "")"
  if [[ -n "$real" ]]; then
    USED_REAL["$real"]=1
  fi
done

# ---------------- process plan ----------------
echo
info "Processing plan..."

# Read header then each CSV row, no subshell so all state is preserved
{
  IFS= read -r header_line || die "Plan file appears to be empty: $PLAN_FILE"

  # Optional: sanity-check header
  #echo "[INFO] Plan header: $header_line"

  while IFS=, read -r osd rotating db wal combined_flag; do
    # Strip trailing CR if plan ever sees CRLF
    osd="${osd%%$'\r'*}"
    rotating="${rotating%%$'\r'*}"
    db="${db%%$'\r'*}"
    wal="${wal%%$'\r'*}"
    combined_flag="${combined_flag%%$'\r'*}"

    # Skip blank lines
    [[ -z "$osd" ]] && continue

    # Expect osd column like "osd1", "osd2", ...
    osd_idx="${osd#osd}"
    if [[ ! "$osd_idx" =~ ^[0-9]+$ ]]; then
      warn "Skipping row with unexpected osd value '$osd'"
      continue
    fi

    # Range filter
    if (( osd_idx < START_OSD || osd_idx > END_OSD )); then
      continue
    fi

    if [[ -z "$rotating" ]]; then
      warn "OSD ${osd} has empty rotating disk path; skipping"
      continue
    fi

    # Resolve real device for rotating disk
    real_rot="$(readlink -f "$rotating" 2>/dev/null || echo "$rotating")"

    # Skip if already used by MicroCeph (by path *or* underlying device)
    if [[ -n "${USED_PATH[$rotating]:-}" || -n "${USED_REAL[$real_rot]:-}" ]]; then
      info "osd${osd_idx}: $rotating (real=$real_rot) already used by MicroCeph; skipping"
      continue
    fi

    # Build microceph disk add command
    cmd=(microceph disk add "$rotating" --wipe)

    combined_lc="$(echo "${combined_flag}" | tr 'A-Z' 'a-z')"

    # Combined DB+WAL on a single device: we just drive DB, and leave WAL unset.
    if [[ "$combined_lc" == "yes" || "$combined_lc" == "true" ]]; then
      if [[ -n "$db" && "$db" != "none" ]]; then
        cmd+=(--db-device "$db" --db-wipe)
      fi
    else
      if [[ -n "$db" && "$db" != "none" ]]; then
        cmd+=(--db-device "$db" --db-wipe)
      fi
      if [[ -n "$wal" && "$wal" != "none" ]]; then
        cmd+=(--wal-device "$wal" --wal-wipe)
      fi
    fi

    if $APPLY; then
      info "osd${osd_idx}: executing: ${cmd[*]}"
      "${cmd[@]}"
    else
      dry "osd${osd_idx}: ${cmd[*]}"
    fi

  done
} < "$PLAN_FILE"

echo
info "Plan processing complete."
if ! $APPLY; then
  echo "Re-run with --apply to actually add these OSDs."
fi
