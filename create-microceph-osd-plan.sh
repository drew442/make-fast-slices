#!/usr/bin/env bash
set -Eeuo pipefail

# create-microceph-osd-plan.sh
#
# Generate a MicroCeph OSD plan CSV to be consumed by apply-microceph-osd-plan.sh.
#
# Inputs:
#   - Fast slices created by make-fast-slices.sh, exposed as symlinks in:
#       /dev/disk/by-mfast/
#     Expected names:
#       metaN        (ignored here; handled separately as metadata OSDs)
#       osdN-db
#       osdN-wal
#       osdN-cmb     (combined DB+WAL on a single fast slice)
#
#   - Rotating disks discovered via:
#       microceph disk list --json --host-only
#     We only use:
#       .AvailableDisks[] with Type == "scsi"
#     and we avoid anything listed in .ConfiguredDisks[].
#
# Output:
#   - CSV written to a persistent standard location:
#       /var/lib/fastmap/microceph-osd-plan.XXXXXX.csv
#
# CSV format (header required, matches apply-microceph-osd-plan.sh):
#   osd,rotating,db,wal,combined_dbwal
#   osd1,/dev/disk/by-id/wwn-...,/dev/disk/by-mfast/osd1-db,/dev/disk/by-mfast/osd1-wal,no
#   osd2,/dev/disk/by-id/wwn-...,/dev/disk/by-mfast/osd2-cmb,none,yes
#
# Behaviour:
#   - One rotating disk is allocated per OSD ID (osd1, osd2, ...), in sorted order.
#   - If osdN-cmb exists, we set combined_dbwal=yes and use that path in the "db" column,
#     with "wal" set to "none".
#   - If separate osdN-db/osdN-wal exist, we set combined_dbwal=no and fill db/wal columns.
#   - If only db *or* wal exists, we set the other to "none" and combined_dbwal=no.
#   - If there are fewer rotating disks than OSD IDs, we fail.
#
# Requirements:
#   - bash 4+
#   - microceph
#   - jq
#   - readlink, basename, sort, mkdir, mktemp

FAST_DIR="/dev/disk/by-mfast"
OUT_ROOT="/var/lib/fastmap"

die(){ echo -e "\e[31m[FATAL]\e[0m $*" >&2; exit 1; }
warn(){ echo -e "\e[33m[WARN]\e[0m  $*" >&2; }
info(){ echo -e "[INFO] $*"; }

usage() {
  cat <<EOF
Usage: $0

Generates a MicroCeph OSD plan CSV based on:
  - Fast slices in: ${FAST_DIR}
  - Rotating disks from: microceph disk list --json --host-only

Output:
  - Plan written to: ${OUT_ROOT}/microceph-osd-plan.XXXXXX.csv

The CSV is compatible with apply-microceph-osd-plan.sh and has columns:
  osd,rotating,db,wal,combined_dbwal

Options:
  -h, --help     Show this help
EOF
}

# ------------- arg parsing -------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    *) die "Unknown option: $1" ;;
  esac
  shift
done

need_cmds=(microceph jq readlink basename sort mkdir mktemp)
for c in "${need_cmds[@]}"; do
  command -v "$c" >/dev/null || die "Missing required command: $c"
done

# ------------- discover fast slices (by-mfast) -------------
declare -A OSD_DB OSD_WAL OSD_CMB

if [[ ! -d "$FAST_DIR" ]]; then
  die "Fast slice directory not found: $FAST_DIR (did make-fast-slices.sh run on this host?)"
fi

info "Scanning fast slices in $FAST_DIR ..."

shopt -s nullglob
for link in "$FAST_DIR"/osd*-*; do
  # only consider symlinks / files; skip weird entries
  [[ -e "$link" ]] || continue

  name="$(basename "$link")"

  case "$name" in
    osd*-db)
      if [[ "$name" =~ ^osd([0-9]+)-db$ ]]; then
        osd_id="${BASH_REMATCH[1]}"
        OSD_DB["$osd_id"]="$link"   # keep by-mfast symlink, do NOT resolve
      fi
      ;;
    osd*-wal)
      if [[ "$name" =~ ^osd([0-9]+)-wal$ ]]; then
        osd_id="${BASH_REMATCH[1]}"
        OSD_WAL["$osd_id"]="$link"
      fi
      ;;
    osd*-cmb)
      if [[ "$name" =~ ^osd([0-9]+)-cmb$ ]]; then
        osd_id="${BASH_REMATCH[1]}"
        OSD_CMB["$osd_id"]="$link"
      fi
      ;;
    *)
      # meta*, etc. are ignored here
      ;;
  esac
done
shopt -u nullglob

# Build list of OSD IDs we have any fast slices for.
declare -a OSD_IDS=()
declare -A OSD_SEEN

for id in "${!OSD_DB[@]}" "${!OSD_WAL[@]}" "${!OSD_CMB[@]}"; do
  [[ -z "$id" ]] && continue
  if [[ -z "${OSD_SEEN[$id]:-}" ]]; then
    OSD_SEEN["$id"]=1
    OSD_IDS+=("$id")
  fi
done

if [[ ${#OSD_IDS[@]} -eq 0 ]]; then
  die "No osdN-* fast slices found in $FAST_DIR (osd*-db / osd*-wal / osd*-cmb)."
fi

# Sort OSD IDs numerically
IFS=$'\n' OSD_IDS=( $(printf "%s\n" "${OSD_IDS[@]}" | sort -n) )
unset IFS

info "Detected OSD fast slices for OSD IDs: ${OSD_IDS[*]}"

# ------------- discover rotating disks via MicroCeph -------------
info "Querying MicroCeph disks via 'microceph disk list --json --host-only'..."
json_out="$(microceph disk list --json --host-only 2>/dev/null || echo "")"
[[ -n "$json_out" ]] || die "microceph disk list --json --host-only returned no output."

# Configured disks (already in use by MicroCeph)
declare -a CONFIGURED_PATHS=()
while IFS= read -r p; do
  [[ -n "$p" ]] && CONFIGURED_PATHS+=("$p")
done < <(printf '%s\n' "$json_out" | jq -r '.ConfiguredDisks[].Path // empty')

declare -A USED_REAL
for p in "${CONFIGURED_PATHS[@]}"; do
  real="$(readlink -f "$p" 2>/dev/null || echo "")"
  [[ -n "$real" ]] && USED_REAL["$real"]=1
done

info "Configured disks on this host: ${#CONFIGURED_PATHS[@]}"

# Candidate rotating disks from AvailableDisks with Type == "scsi"
declare -a ROTATING_CANDIDATES=()
while IFS= read -r p; do
  [[ -z "$p" ]] && continue
  real="$(readlink -f "$p" 2>/dev/null || echo "$p")"
  # Skip anything whose underlying device is already used by MicroCeph
  if [[ -n "${USED_REAL[$real]:-}" ]]; then
    continue
  fi
  ROTATING_CANDIDATES+=("$p")
done < <(printf '%s\n' "$json_out" | jq -r '.AvailableDisks[] | select(.Type=="scsi") | .Path // empty')

if [[ ${#ROTATING_CANDIDATES[@]} -eq 0 ]]; then
  die "No eligible rotating disks found in MicroCeph AvailableDisks (Type==\"scsi\")."
fi

# Sort rotating candidates for deterministic mapping
IFS=$'\n' ROTATING_CANDIDATES=( $(printf "%s\n" "${ROTATING_CANDIDATES[@]}" | sort) )
unset IFS

info "Eligible rotating disks (not yet configured):"
for p in "${ROTATING_CANDIDATES[@]}"; do
  echo "  - $p"
done

# Ensure we have enough rotating disks to cover all OSD IDs
if [[ ${#ROTATING_CANDIDATES[@]} -lt ${#OSD_IDS[@]} ]]; then
  die "Not enough rotating disks (${#ROTATING_CANDIDATES[@]}) for OSDs (${#OSD_IDS[@]})."
fi

# ------------- prepare output file -------------
mkdir -p "$OUT_ROOT"
PLAN_FILE="$(mktemp -p "$OUT_ROOT" microceph-osd-plan.XXXXXX.csv)"

info "Writing plan to: $PLAN_FILE"

# Header matches apply-microceph-osd-plan.sh
echo "osd,rotating,db,wal,combined_dbwal" > "$PLAN_FILE"

# ------------- build plan rows -------------
rot_idx=0

for osd_id in "${OSD_IDS[@]}"; do
  osd_label="osd${osd_id}"

  # assign rotating disk
  if [[ $rot_idx -ge ${#ROTATING_CANDIDATES[@]} ]]; then
    die "Internal error: ran out of rotating disks while assigning OSDs."
  fi
  rotating="${ROTATING_CANDIDATES[$rot_idx]}"
  rot_idx=$((rot_idx + 1))

  db_path="${OSD_DB[$osd_id]:-}"
  wal_path="${OSD_WAL[$osd_id]:-}"
  cmb_path="${OSD_CMB[$osd_id]:-}"
  combined="no"

  if [[ -n "$cmb_path" ]]; then
    # Combined DB+WAL slice: use it as DB, leave WAL as "none", mark combined.
    combined="yes"
    db_path="$cmb_path"
    wal_path="none"
  else
    # Separate DB/WAL or partial:
    [[ -n "$db_path" ]] || db_path="none"
    [[ -n "$wal_path" ]] || wal_path="none"
    combined="no"
  fi

  # CSV-safe: our paths shouldn't contain commas, so we can write directly.
  echo "${osd_label},${rotating},${db_path},${wal_path},${combined}" >> "$PLAN_FILE"
done

echo
info "Plan generation complete."
echo "Plan file: $PLAN_FILE"
echo "You can inspect it with e.g.:"
echo "  column -t -s, \"$PLAN_FILE\""
echo
echo "Apply on this host with (example):"
echo "  sudo ./apply-microceph-osd-plan.sh --plan \"$PLAN_FILE\""