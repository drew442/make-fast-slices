#!/usr/bin/env bash
# Convert vg0/root from linear to RAID1 and wait for sync
set -euxo pipefail
LOG=/var/log/late-lvconvert.log
exec > >(tee -a "$LOG") 2>&1

command -v lvconvert >/dev/null
command -v lvs >/dev/null

# Sanity: show current state
vgdisplay vg0 || true
lvdisplay vg0/root || true
lvs -o lv_name,lv_attr,devices,copy_percent --noheadings vg0 || true

# Convert to RAID1 (two copies total). Requires free extents in vg0 (you should have plenty).
lvconvert --type raid1 -m 1 --yes vg0/root

# Show progress and wait for initial sync (non-fatal if already done)
for i in $(seq 1 180); do
  lvs -o lv_name,lv_attr,devices,copy_percent --noheadings vg0 | sed "s/^ *//" | grep -E "^root" || true
  percent=$(lvs -o copy_percent --noheadings --nosuffix vg0/root 2>/dev/null | tr -d " ")
  if [ -z "$percent" ] || [ "$percent" = "100.00" ]; then
    break
  fi
  sleep 5
done

# Extend to use +85%FREE space in vg0
lvextend -l +85%FREE vg0/root

# Extend the filesystem (assumes ext4)
resize2fs /dev/vg0/root

# Final state
lvs -o lv_name,size,lv_attr,devices,copy_percent vg0
echo "lvconvert to RAID1 and lvextend +85%FREE complete."