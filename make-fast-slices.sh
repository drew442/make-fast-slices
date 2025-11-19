#!/usr/bin/env bash
set -Eeuo pipefail

# make-fast-slices.sh
#
# - creates NVMe namespaces for Ceph DB/WAL + CephFS metadata
# - supports 1+ fast devices, balanced layout
# - dry-run by default; --apply to execute
# - robust against:
#     * nvme list-ns printing "[ 0]:0x1"
#     * controller auto-attaching (Namespace Is Private)
#     * multiple controller IDs, no --controllers=all
#
# Links created in /dev/disk/by-mfast/<label>
#
# CHANGE:
# - we now ALWAYS use `nvme create-ns --block-size=...`
# - user can set global --block-size {512|4096}
# - we DO NOT let the user pick LBAF / FLBAS anymore

APPLY=false
AUTO_DETECT_HDD=false
OSD_COUNT=12

DB_GIB=100
WAL_GIB=6
WAL_SEPARATE=false
COMBINED_GIB=0

META_COUNT=0
META_GIB=256

FAST_DEVS=()

ALLOW_MODIFY_EXISTING_NS=false
WIPE_EXISTING_NS=false
ALLOW_PARTITION=false
WIPE_EXISTING_PARTS=false

MAKE_LINKS=true
LINK_ROOT="/dev/disk/by-mfast"
UDEV_RULE_FILE="/etc/udev/rules.d/99-mfast.rules"

MAP_ROOT="/var/lib/fastmap"

BLOCK_SIZE=512   # can be set with --block-size 4096

need_cmds=(lsblk awk sed grep tr column)
for c in "${need_cmds[@]}"; do command -v "$c" >/dev/null || { echo "Missing $c"; exit 1; }; done
command -v nvme >/dev/null || true
command -v sgdisk >/dev/null || true
command -v sfdisk >/dev/null || true

die(){ echo -e "\e[31m[FATAL]\e[0m $*" >&2; exit 1; }
warn(){ echo -e "\e[33m[WARN]\e[0m  $*" >&2; }
info(){ echo -e "[INFO] $*"; }
ok(){ echo -e "\e[32m[OK]\e[0m    $*"; }
dry(){ echo -e "\e[36m[DRY]\e[0m   $*"; }

usage(){
cat <<EOF
Usage: $0 [options]
  --apply
  --fast-dev /dev/nvme0 [--fast-dev /dev/nvme1 ...]
  --osd-count N
  --db-gib GiB
  --wal-gib GiB
  --wal-separate
  --combined-gib GiB
  --meta-count N
  --meta-gib GiB
  --allow-modify-existing-ns
  --wipe-existing-ns
  --allow-partition
  --wipe-existing-parts
  --no-links
  --block-size 512|4096
EOF
}

# ---------------- arg parsing ----------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply) APPLY=true ;;
    --auto-detect-hdd) AUTO_DETECT_HDD=true ;;
    --osd-count) OSD_COUNT="${2:?}"; shift ;;
    --fast-dev) FAST_DEVS+=("${2:?}"); shift ;;
    --wal-separate) WAL_SEPARATE=true ;;
    --db-gib) DB_GIB="${2:?}"; shift ;;
    --wal-gib) WAL_GIB="${2:?}"; shift ;;
    --combined-gib) COMBINED_GIB="${2:?}"; shift ;;
    --meta-count) META_COUNT="${2:?}"; shift ;;
    --meta-gib) META_GIB="${2:?}"; shift ;;
    --allow-modify-existing-ns) ALLOW_MODIFY_EXISTING_NS=true ;;
    --wipe-existing-ns) WIPE_EXISTING_NS=true ;;
    --allow-partition) ALLOW_PARTITION=true ;;
    --wipe-existing-parts) WIPE_EXISTING_PARTS=true ;;
    --no-links) MAKE_LINKS=false ;;
    --block-size)
        BLOCK_SIZE="${2:?}"
        case "$BLOCK_SIZE" in
          512|4096) ;;  # ok
          *) die "--block-size must be 512 or 4096" ;;
        esac
        shift
        ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown option: $1" ;;
  esac
  shift
done

if $AUTO_DETECT_HDD; then
  OSD_COUNT="$(lsblk -d -o NAME,ROTA,TYPE | awk '$2==1 && $3=="disk" {print $1}' | grep -v '^nvme' | wc -l)"
  info "Auto-detected rotational HDD count: ${OSD_COUNT}"
fi

[[ ${#FAST_DEVS[@]} -ge 1 ]] || die "Add at least one --fast-dev"
(( OSD_COUNT > 0 )) || die "--osd-count must be >0"
if (( COMBINED_GIB > 0 )); then
  WAL_SEPARATE=false
else
  (( DB_GIB > 0 )) || die "--db-gib must be >0 (or use --combined-gib)"
  if $WAL_SEPARATE; then (( WAL_GIB > 0 )) || die "--wal-gib must be >0 with --wal-separate"; fi
fi

# ---------------- helpers ----------------
is_nvme(){
  local d="$1"
  [[ -e "$d" ]] || return 1
  command -v nvme >/dev/null && nvme id-ctrl "$d" &>/dev/null && return 0
  if [[ "$d" =~ ^/dev/nvme[0-9]+n[0-9]+$ ]]; then
    local c="${d%n[0-9]*}"
    nvme id-ctrl "$c" &>/dev/null && return 0
  fi
  return 1
}
get_ctrl_from_ns(){ local d="$1"; [[ "$d" =~ ^/dev/nvme[0-9]+n[0-9]+$ ]] && echo "${d%n[0-9]*}" || echo "$d"; }
require_unused_block() {
  local d="$1"
  # If the device has any mountpoint, hard-fail; otherwise explicitly return 0
  if lsblk -no MOUNTPOINT "$d" | grep -q .; then
    die "$d is mounted"
  fi
  return 0
}
# --- parse nsids from nvme-cli 2.8 style "[ 0]:0x1" ---
list_ns_ids_raw(){
  local ctrl="$1"
  nvme list-ns "$ctrl" 2>/dev/null \
    | awk -F':' '/\[[[:space:]]*[0-9]+\]:/ {gsub(/[[:space:]]/,"",$2); print $2}'
}
list_ns_ids_dec(){
  local ctrl="$1"
  while read -r id; do
    [[ -z "$id" ]] && continue
    if [[ "$id" =~ ^0x[0-9a-fA-F]+$ ]]; then
      echo $((16#${id#0x}))
    elif [[ "$id" =~ ^[0-9]+$ ]]; then
      echo "$id"
    fi
  done < <(list_ns_ids_raw "$ctrl")
}
first_nsid_dec(){ list_ns_ids_dec "$1" | head -n1; }

# --- controller IDs (your drive shows 0x41, 0x42) ---
get_ctrl_ids(){
  local ctrl="$1"
  nvme list-ctrl "$ctrl" 2>/dev/null \
    | awk -F':' '/\[[[:space:]]*[0-9]+\]:/ {gsub(/[[:space:]]/,"",$2); print $2}'
}

bytes_to_h(){
  awk -v b="$1" 'BEGIN{
    split("B KiB MiB GiB TiB PiB", u, " ");
    sign = (b < 0) ? "-" : "";
    if (b < 0) b = -b;
    i = 1;
    while (b >= 1024 && i < 6) { b /= 1024; i++ }
    printf "%s%.2f %s\n", sign, b, u[i];
  }'
}

nvme_total_bytes(){ nvme id-ctrl -H "$1" 2>/dev/null | awk '/tnvmcap/ {gsub(/[^0-9]/,"",$0); print $0; exit}'; }
nvme_used_bytes(){
  local c="$1"
  local lba="$2"
  local sum=0
  while read -r ns; do
    local out ncap
    out="$(nvme id-ns "${c}n${ns}" 2>/dev/null || true)"
    ncap="$(awk -F: '/^ncap/ {gsub(/[[:space:]]/,"",$2); print $2}' <<<"$out")"
    [[ -n "$ncap" ]] && sum=$((sum + ncap * lba))
  done < <(list_ns_ids_dec "$c")
  echo "$sum"
}

block_total_bytes(){ lsblk -b -dn -o SIZE "$1"; }
block_used_bytes(){ local d="$1" sum=0; while read -r n s; do sum=$((sum + s)); done < <(lsblk -b -lno NAME,SIZE "$d" | tail -n +2); echo "$sum"; }

# per-device friendly index (portable)
next_dev_seq() {
  local dev="$1"
  local key="${dev//\//_}"
  local var="DEV_SEQ_${key}"
  local cur
  cur=$(eval "printf '%s' \"\${$var:-0}\"")
  cur=$((cur + 1))
  eval "$var=$cur"
  echo "$cur"
}

ensure_edev_rule() {
  local label="$1"    # e.g. osd1-cmb, meta2, osd3-db
  local target="$2"   # real node, e.g. /dev/sdb2 or /dev/nvme0n3

  local rules_dir="/etc/udev/rules.d"
  local rules_file="$rules_dir/99-mfast.rules"

  mkdir -p "$rules_dir"

  # Grab udev properties for this node
  local props
  if ! props="$(udevadm info --query=property --name="$target" 2>/dev/null)"; then
    warn "udevadm info failed for $target; not installing udev rule for $label (links won't persist across reboot)"
    return 0
  fi

  local devtype wwn partlabel
  devtype="$(sed -n 's/^DEVTYPE=//p' <<<"$props" | head -n1)"
  wwn="$(sed -n 's/^ID_WWN=//p' <<<"$props" | head -n1)"
  partlabel="$(sed -n 's/^ID_PART_ENTRY_NAME=//p' <<<"$props" | head -n1)"

  local rule=""

  # --- Case 1: SAS/SATA SSD partitions (what you showed with sdb2/sdc4) ---
  # Match BOTH the disk WWN and the GPT partition name, so meta1/osd1-cmb/etc
  # each get their correct slice instead of all collapsing onto the same partition.
  if [[ "$devtype" == "partition" && -n "$wwn" && -n "$partlabel" ]]; then
    rule="SUBSYSTEM==\"block\", ENV{DEVTYPE}==\"partition\", ENV{ID_WWN}==\"$wwn\", ENV{ID_PART_ENTRY_NAME}==\"$partlabel\", SYMLINK+=\"disk/by-mfast/$label\""

  # --- Case 2: NVMe namespaces or whole disks (no partition) ---
  elif [[ "$devtype" == "disk" && -n "$wwn" ]]; then
    rule="SUBSYSTEM==\"block\", ENV{DEVTYPE}==\"disk\", ENV{ID_WWN}==\"$wwn\", SYMLINK+=\"disk/by-mfast/$label\""

  # --- Fallback: no WWN/partlabel; try to key off a stable by-id symlink ---
  else
    local byid=""
    for id in /dev/disk/by-id/*; do
      [[ -e "$id" ]] || continue
      if [[ "$(readlink -f "$id")" == "$target" ]]; then
        byid="$(basename "$id")"
        break
      fi
    done

    if [[ -n "$byid" ]]; then
      # DEVLINKS contains a space-separated list of symlinks.
      # Match on the by-id name as a substring.
      rule="SUBSYSTEM==\"block\", ENV{DEVLINKS}==\"*${byid}*\", SYMLINK+=\"disk/by-mfast/$label\""
    else
      warn "Could not derive stable identifier for $target; skipping udev rule for $label"
      return 0
    fi
  fi

  # Avoid duplicate entries
  if [[ -f "$rules_file" ]] && grep -Fq "$rule" "$rules_file"; then
    return 0
  fi

  echo "$rule" >> "$rules_file"
  ok "Installed udev rule for label $label -> $target"

  # Reload udev so rules are active next boot (and now, on re-trigger)
  udevadm control --reload >/dev/null 2>&1 || true
}


ensure_link() {
  local label="$1"
  local target="$2"
  $MAKE_LINKS || return 0

  mkdir -p "$LINK_ROOT"

  # Resolve to real node
  local real link_target
  real="$(readlink -f "$target")"

  # 1) If it's already a concrete block node (nvme namespace, partition), that's fine
  if [[ "$real" =~ ^/dev/nvme[0-9]+n[0-9]+$ || "$real" =~ ^/dev/sd[a-z][0-9]*$ || "$real" =~ ^/dev/nvme[0-9]+p[0-9]+$ ]]; then
    link_target="$real"
  else
    # 2) Try to find a /dev/disk/by-id/ symlink that points to this real node
    local tgt_id=""
    for id in /dev/disk/by-id/*; do
      [[ -e "$id" ]] || continue
      if [[ "$(readlink -f "$id")" == "$real" ]]; then
        case "$id" in
          *nvme-eui.*|*nvme-ns-*) tgt_id="$id"; break ;;
          *) tgt_id="$id" ;;  # keep as candidate
        esac
      fi
    done

    if [[ -n "$tgt_id" ]]; then
      link_target="$tgt_id"
    else
      link_target="$real"
    fi
  fi

  ln -sfn "$link_target" "$LINK_ROOT/$label"
  ok "Link $LINK_ROOT/$label -> $link_target"

  # Also create/update corresponding udev rule so this persists across reboots
  ensure_udev_rule "$label" "$link_target"
}

nvme_create_ns_and_get_nsid(){
  local ctrl="$1" nsze="$2" ncap="$3" block_size="$4" extra="$5"
  local out rc nsid=""
  out="$(nvme create-ns "$ctrl" --nsze="$nsze" --ncap="$ncap" --block-size="$block_size" ${extra:-} 2>&1)" || rc=$?
  rc=${rc:-0}
  [[ $rc -eq 0 ]] || { echo "$out" >&2; return $rc; }

  if [[ "$out" =~ [Nn][Ss][Ii][Dd][[:space:]:=]+(0x[0-9a-fA-F]+|[0-9]+) ]]; then
    nsid="${BASH_REMATCH[1]}"
    [[ "$nsid" =~ ^0x ]] && nsid=$((16#${nsid#0x}))
    echo "$nsid"
    return 0
  fi
  echo ""
  return 0
}

poll_new_nsid_by_diff(){
  local ctrl="$1" before_space="$2"
  local tries=30 nsid=""
  while (( tries-- > 0 )); do
    local after_space; after_space="$(list_ns_ids_dec "$ctrl" | tr '\n' ' ')"
    for cand in $after_space; do
      case " $before_space " in *" $cand "*) ;; *) nsid="$cand"; break;; esac
    done
    [[ -n "$nsid" ]] && { echo "$nsid"; return 0; }
    sleep 0.1
  done
  echo ""
}

attach_ns_safely(){
  local ctrl="$1" nsid="$2"
  local cids; cids="$(get_ctrl_ids "$ctrl" || true)"
  if [[ -z "$cids" ]]; then
    nvme attach-ns "$ctrl" -n "$nsid" >/dev/null 2>&1 || true
    return 0
  fi
  while read -r cid; do
    [[ -z "$cid" ]] && continue
    if ! out="$(nvme attach-ns "$ctrl" -n "$nsid" --controllers="$cid" 2>&1)"; then
      if grep -qi "Namespace Is Private" <<<"$out"; then
        info "nsid=$nsid is already attached to ctrl $cid (private) — continuing"
      else
        warn "attach-ns to ctrl $cid failed: $out"
      fi
    fi
  done <<<"$cids"
}

resolve_ns_path() {
  local ctrl="$1"
  local nsid="$2"

  # Some controllers + udev are slow to create /dev/nvmeXnY and by-id links.
  # Retry a few times, letting udev settle between attempts.
  local tries=30

  while (( tries-- > 0 )); do
    local desc nguid eui
    desc="$(nvme ns-descs "$ctrl" -n "$nsid" 2>/dev/null || true)"
    nguid="$(awk -F': ' '/nguid/ {print $2; exit}' <<<"$desc")"
    eui="$(awk -F': ' '/eui/ {print $2; exit}' <<<"$desc")"

    # Prefer nguid-specific by-id entries first
    if [[ -n "$nguid" ]]; then
      if [[ -e "/dev/disk/by-id/nvme-eui.$nguid" ]]; then
        readlink -f "/dev/disk/by-id/nvme-eui.$nguid"
        return 0
      fi
      if [[ -e "/dev/disk/by-id/nvme-ns-$nguid" ]]; then
        readlink -f "/dev/disk/by-id/nvme-ns-$nguid"
        return 0
      fi
    fi

    # Fallback to EUI if present
    if [[ -n "$eui" ]]; then
      if [[ -e "/dev/disk/by-id/nvme-eui.$eui" ]]; then
        readlink -f "/dev/disk/by-id/nvme-eui.$eui"
        return 0
      fi
    fi

    # Last-resort: classic /dev/nvmeXnY, where Y == nsid
    if [[ -e "${ctrl}n${nsid}" ]]; then
      readlink -f "${ctrl}n${nsid}"
      return 0
    fi

    # Let udev catch up, then try again
    if command -v udevadm >/dev/null 2>&1; then
      udevadm settle --timeout=3 >/dev/null 2>&1 || true
    else
      sleep 0.1
    fi
  done

  # If we get here, we still couldn't resolve the path
  return 1
}


# ---------------- inspect fast devices ----------------
declare -A DEV_KIND NVME_CTRL TOTAL_BYTES BEFORE_USED
for dev in "${FAST_DEVS[@]}"; do
  [[ -e "$dev" ]] || die "Fast device $dev not found"
  if is_nvme "$dev"; then
    DEV_KIND["$dev"]="nvme"
    ctrl="$(get_ctrl_from_ns "$dev")"; NVME_CTRL["$dev"]="$ctrl"

    nslist="$(list_ns_ids_raw "$ctrl" | tr '\n' ' ')"
    if [[ -n "$nslist" ]]; then
      warn "$ctrl has namespaces: $nslist"
      if $WIPE_EXISTING_NS && $ALLOW_MODIFY_EXISTING_NS; then
        if $APPLY; then
          warn "WIPING ALL namespaces on $ctrl"
          while read -r nsid_raw; do
            [[ -z "$nsid_raw" ]] && continue
            nsid_dec="$nsid_raw"; [[ "$nsid_raw" =~ ^0x ]] && nsid_dec=$((16#${nsid_raw#0x}))
            nvme detach-ns "$ctrl" -n "$nsid_dec" >/dev/null 2>&1 || true
            nvme delete-ns "$ctrl" -n "$nsid_dec" >/dev/null 2>&1 || true
          done < <(list_ns_ids_raw "$ctrl")
        else
          dry "Would wipe namespaces on $ctrl: $nslist"
        fi
      fi
    fi
    TOTAL_BYTES["$dev"]="$(nvme_total_bytes "$ctrl" || echo 0)"
    # for the "used" calc we need *some* lba, use the chosen global one
    BEFORE_USED["$dev"]="$(nvme_used_bytes "$ctrl" "$BLOCK_SIZE")"
    info "$dev will create namespaces with block size ${BLOCK_SIZE}B"
  else
    DEV_KIND["$dev"]="block"
    require_unused_block "$dev"
    if ! $ALLOW_PARTITION; then warn "Will not write partitions without --allow-partition"; fi
    if $WIPE_EXISTING_PARTS && $ALLOW_PARTITION; then
      if $APPLY; then
        warn "WIPING ALL partitions on $dev"
        if command -v sgdisk >/dev/null; then sgdisk --zap-all "$dev"; sgdisk -g "$dev"; else sfdisk --delete "$dev" || true; fi
        partprobe "$dev" || true
      else
        dry "Would wipe all partitions on $dev"
      fi
    fi
    TOTAL_BYTES["$dev"]="$(block_total_bytes "$dev" || echo 0)"
    BEFORE_USED["$dev"]="$(block_used_bytes "$dev" || echo 0)"
  fi
done

# ---------------- build plan ----------------
PLAN=()
for i in $(seq 1 "$META_COUNT"); do
  dev="${FAST_DEVS[$(( (i-1) % ${#FAST_DEVS[@]} ))]}"
  PLAN+=("$dev,meta,${META_GIB},meta${i}")
done

db_i=0; wal_i=0
for osd in $(seq 1 "$OSD_COUNT"); do
  if (( COMBINED_GIB > 0 )); then
    dev="${FAST_DEVS[$(( (osd-1) % ${#FAST_DEVS[@]} ))]}"
    PLAN+=("$dev,osd-combined,${COMBINED_GIB},osd${osd}-cmb")
  else
    dev_db="${FAST_DEVS[$(( db_i % ${#FAST_DEVS[@]} ))]}";   db_i=$((db_i+1))
    PLAN+=("$dev_db,osd-db,${DB_GIB},osd${osd}-db")
    if $WAL_SEPARATE; then
      dev_wal="${FAST_DEVS[$(( wal_i % ${#FAST_DEVS[@]} ))]}"; wal_i=$((wal_i+1))
      PLAN+=("$dev_wal,osd-wal,${WAL_GIB},osd${osd}-wal")
    fi
  fi
done

echo
echo "Planned slices (dry-run=$([[ $APPLY == true ]] && echo false || echo true)):"
printf "target,kind,size_gib,label\n"
for row in "${PLAN[@]}"; do echo "$row"; done | column -t -s,

# capacity (pre+planned)
declare -A PLANNED_ADD_BYTES
for row in "${PLAN[@]}"; do
  IFS=, read -r tgt kind size_gib label <<<"$row"
  bytes=$(( size_gib * 1024 * 1024 * 1024 ))
  PLANNED_ADD_BYTES["$tgt"]=$(( ${PLANNED_ADD_BYTES["$tgt"]:-0} + bytes ))
done

echo
echo "Capacity summary (pre + planned):"
printf "device  total  used_before  planned_add  free_after\n"
for dev in "${FAST_DEVS[@]}"; do
  t="${TOTAL_BYTES[$dev]:-0}"; u="${BEFORE_USED[$dev]:-0}"; a="${PLANNED_ADD_BYTES[$dev]:-0}"
  fa=$(( t>0 ? t - (u + a) : 0 ))
  printf "%s  %s  %s  %s  %s\n" "$dev" "$(bytes_to_h "$t")" "$(bytes_to_h "$u")" "$(bytes_to_h "$a")" "$(bytes_to_h "$fa")"
done | column -t

mkdir -p "$MAP_ROOT"
MAP_CSV="$(mktemp -p "$MAP_ROOT" fastmap.XXXXXX.csv)"
echo "object_path,object_type,for_osd,label,size_gib,backend,backend_id" > "$MAP_CSV"

# --- creators ---
create_nvme_ns(){
  local ctrl="$1" size_gib="$2" label="$3" dev_for_maps="$4"
  local bytes=$(( size_gib * 1024 * 1024 * 1024 ))
  local lbas=$(( bytes / BLOCK_SIZE ))
  (( lbas > 0 )) || die "Computed nsze=0; block_size=${BLOCK_SIZE}"

  local before nsid="" path
  before="$(list_ns_ids_dec "$ctrl" | tr '\n' ' ')"

  local cids nmic_arg=""
  cids="$(get_ctrl_ids "$ctrl" || true)"
  if [[ -n "$cids" && "$(wc -l <<<"$cids")" -gt 1 ]]; then
    nmic_arg="--nmic=1"
  fi

  if $APPLY; then
    info "nvme create-ns $ctrl --nsze=$lbas --ncap=$lbas --block-size=${BLOCK_SIZE} ${nmic_arg}"
    nsid="$(nvme_create_ns_and_get_nsid "$ctrl" "$lbas" "$lbas" "$BLOCK_SIZE" "$nmic_arg" || true)"
    if [[ -z "$nsid" ]]; then
      nsid="$(poll_new_nsid_by_diff "$ctrl" "$before")"
    fi
    [[ -n "$nsid" ]] || die "Failed to determine new namespace on $ctrl (create succeeded but nsid not found)"

    attach_ns_safely "$ctrl" "$nsid"

    # give udev a moment to create /dev/nvmeXnY and by-id links
    if command -v udevadm >/dev/null 2>&1; then
      udevadm settle --timeout=3 >/dev/null 2>&1 || true
    else
      sleep 0.1
    fi

    # find the actual device node for this ns
    path="$(resolve_ns_path "$ctrl" "$nsid" || true)"
    
    if [[ -z "$path" ]]; then
      die "Created nsid=$nsid on $ctrl but could not resolve device path"
    fi

    local friendly_id
    friendly_id="$(next_dev_seq "$dev_for_maps")"

    echo "$path,namespace,,${label},${size_gib},nvme,devseq:${friendly_id};nsid:${nsid}" >> "$MAP_CSV"
    ok "NVMe nsid=${nsid} (${size_gib}GiB, ${BLOCK_SIZE}-byte) on $ctrl -> ${path}"
    ensure_link "$label" "$path"
  else
    dry "nvme create-ns $ctrl --nsze=$lbas --ncap=$lbas --block-size=${BLOCK_SIZE}"
    dry "nvme attach-ns $ctrl -n <nsid> (per-controller, ignoring 'Namespace Is Private')"
  fi
}

parttool(){
  local dev="$1" start="$2" size_mib="$3" type_guid="$4" name="$5"
  if command -v sgdisk >/dev/null; then
    if $APPLY; then
      sgdisk --new=0:"$start":"+${size_mib}M" --typecode=0:"$type_guid" --change-name=0:"$name" "$dev"
    else
      dry "sgdisk --new=0:${start}:+${size_mib}M --typecode=0:${type_guid} --change-name=0:${name} $dev"
    fi
  elif command -v sfdisk >/dev/null; then
    local sectors=$(( size_mib * 2048 ))
    if $APPLY; then
      printf ",%s,L\n" "$sectors" | sfdisk --append "$dev"
    else
      dry "sfdisk --append $dev (+${size_mib}MiB)"
    fi
  else
    die "Neither sgdisk nor sfdisk found"
  fi
}

create_block_part(){
  local dev="$1" size_gib="$2" label="$3"
  $ALLOW_PARTITION || die "Refusing to partition $dev without --allow-partition"
  local size_mib=$(( size_gib * 1024 ))
  local start="0" type_guid="8300"
  if $APPLY; then
    parttool "$dev" "$start" "$size_mib" "$type_guid" "$label"
    partprobe "$dev" || true; sleep 0.6
    local path; path="$(lsblk -pnlo NAME "$dev" | tail -n1)"
    [[ -n "$path" ]] || die "Could not find new partition on $dev"
    local friendly_id
    friendly_id="$(next_dev_seq "$dev")"
    echo "$path,partition,,${label},${size_gib},block,devseq:${friendly_id};part" >> "$MAP_CSV"
    ok "Partition ${path} (${size_gib}GiB) on $dev label=${label}"
    ensure_link "$label" "$path"
  else
    dry "Create ~${size_gib}GiB partition on $dev label=${label}"
  fi
}

# --- execute plan ---
echo
for row in "${PLAN[@]}"; do
  IFS=, read -r tgt kind size_gib label <<<"$row"
  backend="${DEV_KIND[$tgt]}"
  if [[ "$backend" == "nvme" ]]; then
    ctrl="${NVME_CTRL[$tgt]:-$tgt}"
    create_nvme_ns "$ctrl" "$size_gib" "$label" "$tgt"
  else
    create_block_part "$tgt" "$size_gib" "$label"
  fi
done

# after apply capacity
if $APPLY; then
  echo
  echo "Capacity summary (after apply):"
  printf "device  total  used_after  free_after\n"
  for dev in "${FAST_DEVS[@]}"; do
    t="${TOTAL_BYTES[$dev]:-0}"
    if [[ "${DEV_KIND[$dev]}" == "nvme" ]]; then
      ctrl="${NVME_CTRL[$dev]}"
      ua="$(nvme_used_bytes "$ctrl" "$BLOCK_SIZE")"
    else
      ua="$(block_used_bytes "$dev")"
    fi
    fa=$(( t>0 ? t - ua : 0 ))
    printf "%s  %s  %s  %s\n" "$dev" "$(bytes_to_h "$t")" "$(bytes_to_h "$ua")" "$(bytes_to_h "$fa")"
  done | column -t
fi

if $APPLY; then
  # Reload udev rules so newly written mfast rules take effect immediately
  if command -v udevadm >/dev/null 2>&1; then
    info "Reloading udev rules and triggering block devices..."
    udevadm control --reload-rules || true
    udevadm trigger --subsystem-match=block || true
  fi
fi

echo
if $APPLY; then
  ok "All planned slices created."
  echo "Mapping CSV: $MAP_CSV"
  column -t -s, "$MAP_CSV" || cat "$MAP_CSV"
else
  dry "This was a dry-run. Re-run with --apply to execute."
  echo "Planned mapping (on apply) will be written to: $MAP_CSV"
fi