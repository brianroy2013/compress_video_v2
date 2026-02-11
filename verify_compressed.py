#!/usr/bin/env python3
import json, os, subprocess, sys
from pathlib import Path

BACKUP_BASE = Path("/home/brian/share/orig_video_to_delete")
ENCODER_TAG = "compressed_hevc_cq26"

def human_size(nbytes):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024.0:
            return f"{nbytes:.2f} {unit}"
        nbytes /= 1024.0
    return f"{nbytes:.2f} PB"

def check_metadata_tag(fp):
    try:
        r = subprocess.run(["ffprobe","-v","quiet","-print_format","json","-show_entries","format_tags=comment",str(fp)], capture_output=True, text=True, timeout=30)
        if r.returncode != 0: return False, "ffprobe failed"
        data = json.loads(r.stdout)
        c = data.get("format",{}).get("tags",{}).get("comment","")
        if ENCODER_TAG in c: return True, None
        return False, f"comment={repr(c)}" if c else (False, "no comment tag")
    except subprocess.TimeoutExpired: return False, "timeout"
    except Exception as e: return False, str(e)

def check_playable(fp):
    try:
        r = subprocess.run(["ffprobe","-v","error","-show_entries","stream=codec_type","-print_format","json",str(fp)], capture_output=True, text=True, timeout=30)
        if r.returncode != 0: return False, r.stderr.strip()[:200]
        data = json.loads(r.stdout)
        streams = data.get("streams",[])
        if not streams: return False, "no streams"
        types = [s.get("codec_type") for s in streams]
        if "video" not in types: return False, f"no video: {types}"
        return True, None
    except subprocess.TimeoutExpired: return False, "timeout"
    except Exception as e: return False, str(e)

def main():
    print(f"Scanning: {BACKUP_BASE}")
    bf = []
    for root, dirs, files in os.walk(BACKUP_BASE):
        for f in files: bf.append(Path(root)/f)
    total = len(bf)
    print(f"Found {total} backed-up original files.")
    safe, missing, issues = [], [], []
    for i, bp in enumerate(sorted(bf), 1):
        rel = bp.relative_to(BACKUP_BASE)
        op = Path("/") / rel
        if i % 200 == 0 or i == total or i == 1:
            print(f"  Checking {i}/{total}...", flush=True)
        if not op.exists():
            missing.append((bp, op)); continue
        probs = []
        ok, msg = check_metadata_tag(op)
        if not ok: probs.append(f"NO TAG: {msg}")
        bs = bp.stat().st_size
        cs = op.stat().st_size
        if cs >= bs: probs.append(f"SIZE: compressed ({human_size(cs)}) >= original ({human_size(bs)})")
        ok2, msg2 = check_playable(op)
        if not ok2: probs.append(f"NOT PLAYABLE: {msg2}")
        if probs: issues.append((bp, op, probs, bs))
        else: safe.append((bp, op, bs, cs))
    tss = sum(s[2] for s in safe)
    tsv = sum(s[2]-s[3] for s in safe)
    print()
    print("=" * 80)
    print("VERIFICATION SUMMARY")
    print("=" * 80)
    print(f"Total files checked:              {total}")
    print(f"Compressed version MISSING:       {len(missing)}")
    print(f"Compressed exists but has ISSUES:  {len(issues)}")
    print(f"Safe to delete (valid compressed): {len(safe)}")
    print(f"Space used by safe-to-delete originals: {human_size(tss)}")
    print(f"Space saved by compression (safe files): {human_size(tsv)}")
    if missing:
        print()
        print(f"MISSING COMPRESSED FILES ({len(missing)}):")
        print("-" * 60)
        for bp2, op2 in missing:
            bsz = bp2.stat().st_size
            print(f"  Should be at: {op2}")
            print(f"    Backup: {bp2}  Size: {human_size(bsz)}")
    if issues:
        print()
        print(f"FILES WITH ISSUES ({len(issues)}):")
        print("-" * 60)
        for bp2, op2, probs2, bsz in issues:
            print(f"  Compressed: {op2}")
            print(f"    Backup: {bp2}  ({human_size(bsz)})")
            for p in probs2: print(f"    - {p}")
    print("=" * 80)
    if not missing and not issues:
        print(f"ALL {total} files have valid compressed counterparts.")
        print(f"Safe to delete backup dir to free {human_size(tss)}.")
    else:
        pc = len(missing) + len(issues)
        print(f"{len(safe)} of {total} safe to delete ({human_size(tss)}).")
        print(f"{pc} files need attention before deleting.")
    print("=" * 80)

if __name__ == "__main__": main()
