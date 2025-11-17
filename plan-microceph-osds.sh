#!/usr/bin/env bash
set -Eeuo pipefail

# plan-microceph-osds.sh
#
# Read /dev/disk/by-mfast (from make-fast-slices.sh) and the system's
# rotating disks to generate a *plan* for MicroCeph OSD creation.
#
# - Uses /dev/disk/by-id/wwn-* for data (rotating disks).
# - Uses /dev/disk/by-mfast/osdN-db and /dev/disk/by-mfast/osdN-wal for DB/WAL.
# - Detects existing MicroCeph disks via `microceph disk list` (if available)
#   and excludes those devices from the candidate HDD list.
# - Does NOT perform any destructive operations; it only prints a plan and
#   writes a CSV to /tmp.
#
# Usage:
#   sudo ./plan-microceph-osds.sh
#
# After running, inspect the printed table & suggested commands, and
# then manually run the `microceph disk add` lines you want.

# ---------------- basic helpers ----------------

die(){ echo -e "\e[31m[FATAL]\e[0m $*" >&2; exit 1; }
warn(){ echo -e "\e[33m[WARN]\e[0m  $*" >&2; }
info(){ echo "[INFO] $*"; }
ok(){ echo -e "\e[32m[OK]\e[0m    $*"; }

need_cmds=(lsblk awk sed grep tr readlink)
for c in "${need_cmds[@]}"; do
  command -v "$c" >/dev/null 2>&1 || die "Missing required command: $c"
done

MFAST_DIR="/dev/disk/by-mfast"

[[ -d "$MFAST_DIR" ]] || die "$MFAST_DIR does not exist. Run make-fast-slices.sh first."

# ---------------- parse mfast entries ----------------

declare -A OSD_DB OSD_WAL OSD_CMB
max_osd_index=0

# We care about symlinks named: osd<NUM>-db, osd<NUM>-wal, osd<NUM>-cmb
while IFS= read -r -d '' link; do
  base="$(basename "$link")"
  # match osd<number>-suffix
  if [[ "$base" =~ ^osd([0-9]+)-(db|wal|cmb)$ ]]; then
    osd_idx="${BASH_REMATCH[1]}"
    kind="${BASH_REMATCH[2]}"
    (( osd_idx > max_osd_index )) && max_osd_index="$osd_idx"

    real="$(readlink -f "$link" 2>/dev/null || true)"
    [[ -z "$real" ]] && { warn "Cannot resolve $link, skipping"; continue; }

    case "$kind" in
      db)  OSD_DB["$osd_idx"]="$link"  ;;   # keep by-mfast path (more stable)
      wal) OSD_WAL["$osd_idx"]="$link" ;;
      cmb) OSD_CMB["$osd_idx"]="$link" ;;
    esac
  fi
done < <(find "$MFAST_DIR" -maxdepth 1 -type l -print0)

if (( max_osd_index == 0 )); then
  die "No osdN-db/osdN-wal/osdN-cmb links found under $MFAST_DIR"
fi

ok "Found fast slices for OSD indexes 1..${max_osd_index} (some indexes may be sparse)"

# ---------------- detect existing MicroCeph disks ----------------

declare -A USED_REAL

if command -v microceph >/dev/null 2>&1; then
  info "Detecting existing MicroCeph disks via 'microceph disk list'..."
  # Very loose parsing: grab anything that looks like /dev/...
  while IFS= read -r line; do
    # pull all /dev/... tokens from the line
    tmp="$line"
    while [[ "$tmp" =~ (/dev/[^[:space:]]+) ]]; do
      dev="${BASH_REMATCH[1]}"
      real="$(readlink -f "$dev" 2>/dev/null || echo "$dev")"
      USED_REAL["$real"]=1
      # move past this match
      tmp="${tmp#*${dev}}"
    done
  done < <(microceph disk list 2>/dev/null || true)
else
  warn "microceph command not found; assuming no existing MicroCeph disks"
fi

# ---------------- find rotating disks & map to wwn-* ----------------

info "Discovering rotating (ROTA=1) disks..."

# We'll collect candidate HDDs here as their /dev/disk/by-id/wwn-* path
declare -a HDD_BYID
declare -A HDD_REAL_FROM_BYID  # byid -> real node (/dev/sdX or /dev/nvmeXn1 etc.)

# list block devices (excluding RAM, loop, etc.)
while read -r name rota type; do
  [[ "$type" != "disk" ]] && continue
  [[ "$rota" != "1" ]] && continue  # only rotational

  real_dev="/dev/$name"

  # find wwn-* symlink that points to this real_dev
  byid=""
  while IFS= read -r -d '' idlink; do
    target="$(readlink -f "$idlink" 2>/dev/null || true)"
    [[ -z "$target" ]] && continue
    if [[ "$target" == "$real_dev" ]]; then
      byid="$idlink"
      break
    fi
  done < <(find /dev/disk/by-id/ -maxdepth 1 -type l -name 'wwn-*' -print0 2>/dev/null || true)

  if [[ -z "$byid" ]]; then
    warn "No /dev/disk/by-id/wwn-* symlink found for $real_dev; skipping this disk"
    continue
  fi

  real_resolved="$(readlink -f "$byid" 2>/dev/null || echo "$real_dev")"

  # skip if already used by MicroCeph
  if [[ -n "${USED_REAL[$real_resolved]:-}" ]]; then
    info "Skipping $byid (real=$real_resolved) because MicroCeph already uses this disk"
    continue
  fi

  HDD_BYID+=("$byid")
  HDD_REAL_FROM_BYID["$byid"]="$real_resolved"
done < <(lsblk -dn -o NAME,ROTA,TYPE 2>/dev/null)

if ((${#HDD_BYID[@]} == 0)); then
  die "No candidate rotating disks (with wwn-* and not used by MicroCeph) found."
fi

# sort HDD list for stable ordering
IFS=$'\n' HDD_BYID=($(printf '%s\n' "${HDD_BYID[@]}" | sort)) ; unset IFS

ok "Found ${#HDD_BYID[@]} candidate spinning data disks."

# ---------------- build OSD plan ----------------

# We'll plan at most min(max_osd_index, number of HDDs) OSDs,
# to avoid creating OSDs with no data disk.
num_osd_planned="${max_osd_index}"
if ((${#HDD_BYID[@]} < num_osd_planned)); then
  warn "Have fast slices up to osd${max_osd_index}, but only ${#HDD_BYID[@]} HDDs."
  warn "Limiting OSD plan to ${#HDD_BYID[@]} OSDs."
  num_osd_planned="${#HDD_BYID[@]}"
fi

if (( num_osd_planned == 0 )); then
  die "No OSDs can be planned (no overlap between fast slices and HDDs)."
fi

ok "Planning ${num_osd_planned} OSD(s)."

# Data structure for plan: arrays indexed from 1..num_osd_planned
declare -A PLAN_DATA PLAN_DB PLAN_WAL PLAN_DBWAL_COMBINED

for ((i=1; i<=num_osd_planned; i++)); do
  data_byid="${HDD_BYID[$((i-1))]}"
  PLAN_DATA["$i"]="$data_byid"

  # prefer combined if present
  if [[ -n "${OSD_CMB[$i]:-}" ]]; then
    PLAN_DB["$i"]="${OSD_CMB[$i]}"
    PLAN_WAL["$i"]="${OSD_CMB[$i]}"
    PLAN_DBWAL_COMBINED["$i"]="yes"
  else
    PLAN_DB["$i"]="${OSD_DB[$i]:-}"
    PLAN_WAL["$i"]="${OSD_WAL[$i]:-}"
    PLAN_DBWAL_COMBINED["$i"]="no"
  fi
done

# ---------------- output plan (human readable) ----------------

echo
echo "MicroCeph OSD plan:"
printf '%-6s %-45s %-35s %-35s %-8s\n' "OSD#" "DATA (by-id)" "DB (by-mfast)" "WAL (by-mfast)" "CMB?"
echo "------------------------------------------------------------------------------------------------------------------------------------------"

for ((i=1; i<=num_osd_planned; i++)); do
  data="${PLAN_DATA[$i]}"
  db="${PLAN_DB[$i]:-}"
  wal="${PLAN_WAL[$i]:-}"
  cmb="${PLAN_DBWAL_COMBINED[$i]:-no}"

  [[ -z "$data" ]] && continue

  printf '%-6s %-45s %-35s %-35s %-8s\n' \
    "osd$i" \
    "$data" \
    "${db:-"-"}" \
    "${wal:-"-"}" \
    "$cmb"
done

echo
echo "Suggested microceph commands (DATA only; DB/WAL are comments for your reference):"
for ((i=1; i<=num_osd_planned; i++)); do
  data="${PLAN_DATA[$i]}"
  [[ -z "$data" ]] && continue
  db="${PLAN_DB[$i]:-}"
  wal="${PLAN_WAL[$i]:-}"

  comment_parts=()
  [[ -n "$db" ]] && comment_parts+=("db=$db")
  [[ -n "$wal" ]] && comment_parts+=("wal=$wal")

  comment=""
  if ((${#comment_parts[@]} > 0)); then
    comment="# ${comment_parts[*]}"
  fi

  echo "microceph disk add \"$data\"  $comment"
done

# ---------------- write CSV ----------------

CSV_PATH="$(mktemp -t microceph-osd-plan.XXXXXX).csv"
{
  echo "osd_id,data_dev,db_dev,wal_dev,combined_dbwal"
  for ((i=1; i<=num_osd_planned; i++)); do
    data="${PLAN_DATA[$i]}"
    [[ -z "$data" ]] && continue
    db="${PLAN_DB[$i]:-}"
    wal="${PLAN_WAL[$i]:-}"
    cmb="${PLAN_DBWAL_COMBINED[$i]:-no}"

    echo "osd${i},${data},${db},${wal},${cmb}"
  done
} > "$CSV_PATH"

echo
ok "Wrote machine-readable plan to: $CSV_PATH"
echo "You can import this CSV into other tooling or scripts as needed."
