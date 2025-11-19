#!/usr/bin/env bash
set -Eeuo pipefail

# create-microceph-osd-plan.sh
#
# Build a MicroCeph OSD plan based on:
#   - NVMe slices in /dev/disk/by-mfast (osdX-db / osdX-wal)
#   - Rotating disks from `microceph disk list --json --host-only` (AvailableDisks, Type="scsi")
#
# Output CSV (to stdout or --out file):
#   osd,rotating,db,wal,combined_dbwal
#
# Notes:
#   - Only uses "AvailableDisks" (so anything already configured as an OSD is ignored).
#   - Rotating disks are always /dev/disk/by-id/wwn-* where Type == "scsi".
#   - DB/WAL devices are /dev/disk/by-mfast/osdN-db and /dev/disk/by-mfast/osdN-wal if present.
#   - combined_dbwal is "no" for DB+WAL separate, "yes" if db-only is present and wal is empty.
OUT_ROOT="/var/lib/fastmap"
mkdir -p "$OUT_ROOT"
MAP_CSV="$(mktemp -p "$OUT_ROOT" microceph-osd-plan.XXXXXX.csv)"
OUT_FILE=""
MAX_OSDS=0        # 0 = "as many as we can"
HOST_ONLY=true

die(){ echo -e "\e[31m[FATAL]\e[0m $*" >&2; exit 1; }
warn(){ echo -e "\e[33m[WARN]\e[0m  $*" >&2; }
info(){ echo -e "[INFO] $*"; }

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --max-osds N     Limit the number of OSDs to N (default: use all candidates)
  --no-host-only   Don't pass --host-only to microceph disk list (normally you want host-only)
  -h, --help       Show this help

Example:
  $0 --max-osds 5
EOF
}

need_cmds=(microceph jq readlink lsblk)
for c in "${need_cmds[@]}"; do
  command -v "$c" >/dev/null || die "Missing required command: $c"
done

# ---------- arg parsing ----------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --max-osds) MAX_OSDS="${2:?}"; shift ;;
    --no-host-only) HOST_ONLY=false ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown option: $1" ;;
  esac
  shift
done

if [[ -n "$MAX_OSDS" && "$MAX_OSDS" -lt 0 ]]; then
  die "--max-osds must be >= 0"
fi

# ---------- discover microceph disks ----------
info "Querying MicroCeph disks via 'microceph disk list --json${HOST_ONLY:+ --host-only}'..."

if $HOST_ONLY; then
  json_out="$(microceph disk list --json --host-only 2>/dev/null || echo "")"
else
  json_out="$(microceph disk list --json 2>/dev/null || echo "")"
fi

[[ -n "$json_out" ]] || die "Failed to get JSON from 'microceph disk list'"

# ConfiguredDisks: we don't ever use these as new rotating disks
declare -a CONFIGURED_PATHS=()
while IFS= read -r p; do
  [[ -n "$p" ]] && CONFIGURED_PATHS+=("$p")
done < <(printf '%s\n' "$json_out" | jq -r '.ConfiguredDisks[].Path // empty')

info "Configured MicroCeph disks on this host: ${#CONFIGURED_PATHS[@]}"

declare -A USED_PATH USED_REAL
for p in "${CONFIGURED_PATHS[@]}"; do
  USED_PATH["$p"]=1
  real="$(readlink -f "$p" 2>/dev/null || echo "")"
  [[ -n "$real" ]] && USED_REAL["$real"]=1
done

# AvailableDisks: these are our candidates
# We only want:
#   - Type == "scsi"
#   - Path matches /dev/disk/by-id/wwn-*
#   - Not already in USED_REAL / USED_PATH (paranoid, though AvailableDisks should already exclude)
declare -a ROTATING=()
while IFS= read -r path; do
  [[ -z "$path" ]] && continue
  # sanity: ensure it's a wwn path
  if [[ "$path" != /dev/disk/by-id/wwn-* ]]; then
    continue
  fi
  real="$(readlink -f "$path" 2>/dev/null || echo "$path")"
  if [[ -n "${USED_PATH[$path]:-}" || -n "${USED_REAL[$real]:-}" ]]; then
    info "Skipping rotating disk already configured: $path (real=$real)"
    continue
  fi
  ROTATING+=("$path")
done < <(printf '%s\n' "$json_out" | jq -r '
  .AvailableDisks[]
  | select(.Type == "scsi")
  | .Path // empty
')

if ((${#ROTATING[@]} == 0)); then
  die "No suitable rotating disks found in AvailableDisks (Type=scsi, /dev/disk/by-id/wwn-*)."
fi

info "Found ${#ROTATING[@]} candidate rotating disk(s) for OSD data."

# ---------- discover DB/WAL slices in /dev/disk/by-mfast ----------
MF_ROOT="/dev/disk/by-mfast"

if [[ ! -d "$MF_ROOT" ]]; then
  die "$MF_ROOT does not exist; you need to run make-fast-slices.sh first."
fi

# Map osd index -> db path / wal path
declare -A DB_PATH WAL_PATH

# osdX-db
while IFS= read -r p; do
  [[ -z "$p" ]] && continue
  base="$(basename "$p")"   # e.g. osd3-db
  if [[ "$base" =~ ^osd([0-9]+)-db$ ]]; then
    idx="${BASH_REMATCH[1]}"
    DB_PATH["$idx"]="$p"
  fi
done < <(find "$MF_ROOT" -maxdepth 1 -type l -name 'osd*-db' 2>/dev/null | sort -V)

# osdX-wal
while IFS= read -r p; do
  [[ -z "$p" ]] && continue
  base="$(basename "$p")"   # e.g. osd3-wal
  if [[ "$base" =~ ^osd([0-9]+)-wal$ ]]; then
    idx="${BASH_REMATCH[1]}"
    WAL_PATH["$idx"]="$p"
  fi
done < <(find "$MF_ROOT" -maxdepth 1 -type l -name 'osd*-wal' 2>/dev/null | sort -V)

# Count max index we have DB/WAL slices for
max_idx=0
for k in "${!DB_PATH[@]}"; do
  (( k > max_idx )) && max_idx="$k"
done
for k in "${!WAL_PATH[@]}"; do
  (( k > max_idx )) && max_idx="$k"
done

if (( max_idx == 0 )); then
  warn "No osdX-db/osdX-wal labels found in $MF_ROOT; DB/WAL columns will be empty."
fi

# Number of OSDs we *can* plan for is limited by both rotating disks and index range.
max_osds_by_rot=${#ROTATING[@]}
max_osds=$max_osds_by_rot
if (( max_idx > 0 && max_idx < max_osds )); then
  max_osds="$max_idx"
fi
if (( MAX_OSDS > 0 && MAX_OSDS < max_osds )); then
  max_osds="$MAX_OSDS"
fi

if (( max_osds == 0 )); then
  die "Nothing to plan: no overlapping rotating disks and DB/WAL labels, and/or --max-osds=0."
fi

info "Planning for up to ${max_osds} OSD(s)."

# ---------- emit CSV plan ----------
if [[ -n "$OUT_FILE" ]]; then
  exec >"$OUT_FILE"
fi

echo "osd,rotating,db,wal,combined_dbwal"

for ((i=1; i<=max_osds; i++)); do
  # rotating disk for this OSD
  # Use round-robin over ROTATING (in case more NVMe slices than HDDs / or vice versa)
  rot="${ROTATING[$(( (i-1) % ${#ROTATING[@]} ))]}"

  db="${DB_PATH[$i]:-}"
  wal="${WAL_PATH[$i]:-}"

  combined="no"
  # if we have a DB slice but no WAL slice, mark combined=yes (DB-only)
  if [[ -n "$db" && -z "$wal" ]]; then
    combined="yes"
  fi

  echo "osd${i},${rot},${db},${wal},${combined}"
done

if [[ -n "$OUT_FILE" ]]; then
  info "Plan written to $OUT_FILE"
else
  info "Plan written to stdout"
fi
