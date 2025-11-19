#!/usr/bin/env bash
set -Eeuo pipefail

# create-microceph-osd-plan.sh
# - Build an OSD plan for MicroCeph using:
#   * rotating "data" disks (scsi /dev/disk/by-id/wwn-*)
#   * fast slices in /dev/disk/by-mfast (osdN-db, osdN-wal, osdN-cmb)
#
# Output:
#   CSV written to /var/lib/fastmap/microceph-osd-plan.XXXXXX.csv
#   Columns:
#     osd_id,data_path,db_path,wal_path,combined_dbwal
#
# Requirements:
#   - microceph
#   - jq
#
# Notes:
#   - Only disks from "AvailableDisks" in `microceph disk list --json --host-only`
#     are considered for data_path (so existing MicroCeph disks are ignored).
#   - combined_dbwal = "yes" means db_path == wal_path (osdN-cmb).
#   - If only osdN-db exists (no wal/db combo slice), combined_dbwal="no"
#     and wal_path is empty.

OUT_ROOT="/var/lib/fastmap"
mkdir -p "$OUT_ROOT"
OUT_FILE="$(mktemp -p "$OUT_ROOT" microceph-osd-plan.XXXXXX.csv)"

FAST_DIR="/dev/disk/by-mfast"

die() { echo -e "\e[31m[FATAL]\e[0m $*" >&2; exit 1; }
warn(){ echo -e "\e[33m[WARN]\e[0m  $*" >&2; }
info(){ echo -e "[INFO] $*"; }

usage() {
  cat <<EOF
Usage: $0

Creates a MicroCeph OSD plan CSV at:
  $OUT_FILE

CSV columns:
  osd_id,data_path,db_path,wal_path,combined_dbwal

No options are currently supported.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

command -v microceph >/dev/null 2>&1 || die "microceph command not found"
command -v jq >/dev/null 2>&1 || die "jq command not found"

[[ -d "$FAST_DIR" ]] || warn "$FAST_DIR does not exist; proceeding without fast slices."

info "Plan file will be: $OUT_FILE"

# -----------------------------------------------------------
# 1. Query MicroCeph disks (JSON)
# -----------------------------------------------------------

info "Querying MicroCeph disks via 'microceph disk list --json --host-only'..."
MC_JSON="$(microceph disk list --json --host-only)"

# Configured disks (for reference; we don't plan to reuse these)
CONFIGURED_PATHS=()
while IFS= read -r p; do
  [[ -n "$p" ]] && CONFIGURED_PATHS+=("$p")
done < <(printf '%s\n' "$MC_JSON" | jq -r '.ConfiguredDisks[].Path // empty')

info "Detected ${#CONFIGURED_PATHS[@]} configured disk(s) in MicroCeph on this host."

# Available data disks (Type == "scsi", wwn-* only)
DATA_DISKS=()
while IFS= read -r p; do
  [[ -n "$p" ]] && DATA_DISKS+=("$p")
done < <(printf '%s\n' "$MC_JSON" \
  | jq -r '.AvailableDisks[]
           | select(.Type=="scsi")
           | .Path
           | select(startswith("/dev/disk/by-id/wwn-"))')

if ((${#DATA_DISKS[@]} == 0)); then
  warn "No available scsi /dev/disk/by-id/wwn-* disks found in MicroCeph output."
  warn "Plan will be empty."
fi

# Sort data disks for deterministic OSD assignment
if ((${#DATA_DISKS[@]} > 0)); then
  mapfile -t DATA_DISKS < <(printf '%s\n' "${DATA_DISKS[@]}" | sort)
fi

info "Found ${#DATA_DISKS[@]} candidate data disk(s) for OSDs."

# -----------------------------------------------------------
# 2. Discover fast slices in /dev/disk/by-mfast
# -----------------------------------------------------------

declare -A OSD_DB OSD_WAL OSD_CMB

if [[ -d "$FAST_DIR" ]]; then
  shopt -s nullglob
  for link in "$FAST_DIR"/osd*-*; do
    name="$(basename "$link")"
    case "$name" in
      osd*-db)
        if [[ "$name" =~ ^osd([0-9]+)-db$ ]]; then
          osd_id="${BASH_REMATCH[1]}"
          OSD_DB["$osd_id"]="$(readlink -f "$link")"
        fi
        ;;
      osd*-wal)
        if [[ "$name" =~ ^osd([0-9]+)-wal$ ]]; then
          osd_id="${BASH_REMATCH[1]}"
          OSD_WAL["$osd_id"]="$(readlink -f "$link")"
        fi
        ;;
      osd*-cmb)
        if [[ "$name" =~ ^osd([0-9]+)-cmb$ ]]; then
          osd_id="${BASH_REMATCH[1]}"
          OSD_CMB["$osd_id"]="$(readlink -f "$link")"
        fi
        ;;
      *)
        # ignore meta*, other labels
        ;;
    esac
  done
  shopt -u nullglob
fi

# Determine max OSD index implied by fast slices
max_osd_id=0

for k in "${!OSD_DB[@]}"; do
  (( k > max_osd_id )) && max_osd_id="$k"
done
for k in "${!OSD_WAL[@]}"; do
  (( k > max_osd_id )) && max_osd_id="$k"
done
for k in "${!OSD_CMB[@]}"; do
  (( k > max_osd_id )) && max_osd_id="$k"
done

# We will not create more OSDs than we have data disks
if ((${#DATA_DISKS[@]} < max_osd_id)); then
  max_osd_id=${#DATA_DISKS[@]}
fi

if ((max_osd_id == 0)); then
  info "No OSD indices found from fast slices; plan will only use data disks if any."
  max_osd_id=${#DATA_DISKS[@]}
fi

if ((max_osd_id == 0)); then
  warn "No OSDs can be planned (no data disks and no fast slices)."
fi

info "Planning OSDs for indices 1..${max_osd_id}"

# -----------------------------------------------------------
# 3. Write CSV plan
# -----------------------------------------------------------

{
  echo "osd_id,data_path,db_path,wal_path,combined_dbwal"

  for ((osd=1; osd<=max_osd_id; osd++)); do
    # Data disk for this OSD (0-based index into DATA_DISKS)
    data_path=""
    if (( osd <= ${#DATA_DISKS[@]} )); then
      data_path="${DATA_DISKS[$((osd-1))]}"
    fi

    db_path=""
    wal_path=""
    combined="no"

    # Priority:
    # 1. combined slice (osdN-cmb)
    # 2. separate db + wal (osdN-db, osdN-wal)
    # 3. db only (no wal)
    if [[ -n "${OSD_CMB[$osd]:-}" ]]; then
      db_path="${OSD_CMB[$osd]}"
      wal_path="${OSD_CMB[$osd]}"
      combined="yes"
    else
      if [[ -n "${OSD_DB[$osd]:-}" ]]; then
        db_path="${OSD_DB[$osd]}"
      fi
      if [[ -n "${OSD_WAL[$osd]:-}" ]]; then
        wal_path="${OSD_WAL[$osd]}"
      fi
    fi

    # If we have no data_path and no fast path at all, skip row
    if [[ -z "$data_path" && -z "$db_path" && -z "$wal_path" ]]; then
      continue
    fi

    echo "${osd},${data_path},${db_path},${wal_path},${combined}"
  done

} > "$OUT_FILE"

info "Plan written to: $OUT_FILE"
echo
echo "Preview:"
echo "------------------------------------------------------------"
column -t -s, "$OUT_FILE" || cat "$OUT_FILE"
echo "------------------------------------------------------------"
