#!/usr/bin/env python3
"""
Video Compression v3 - Filesystem-based tracking (no database).

File naming conventions replace the database:
    {stem}_compressed.mp4  — successfully compressed/converted
    {stem}_skip.mp4        — skipped (strategy decision or failed size gate)

Subcommands:
    run [folders...]        Discover + process in one pass
    run --dry-run           Show what would happen
    status [folders...]     Walk filesystem, count files by state
    migrate [folders...]    Rename v2-tagged files to _compressed.mp4
"""

import argparse
import socket
import sys
import time
from pathlib import Path

import claim
import encode
import strategy

VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.mpeg', '.mpg'}
HOSTNAME = socket.gethostname()


def human_size(nbytes):
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if abs(nbytes) < 1024.0:
            return f'{nbytes:.2f} {unit}'
        nbytes /= 1024.0
    return f'{nbytes:.2f} PB'


def is_processable(path):
    """Check if a file should be considered for processing.

    Skips files that:
    - Have _skip in the stem
    - Are hidden (start with .)
    - Are temp files (.tmp)
    """
    name = path.name
    stem = path.stem.lower()
    if name.startswith('.'):
        return False
    if '.tmp' in name.lower():
        return False
    if '_skip' in stem:
        return False
    return True


def discover_folders(folders):
    """Walk folders and return sorted list of directories containing processable videos.

    Only checks filenames/extensions — no ffprobe calls, so this is fast.
    """
    dirs_with_videos = set()
    for folder in folders:
        folder = Path(folder).resolve()
        if not folder.is_dir():
            print(f'Warning: {folder} is not a directory, skipping')
            continue
        for path in folder.rglob('*'):
            if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS and is_processable(path):
                dirs_with_videos.add(path.parent)
    return sorted(dirs_with_videos)


def scan_folder(folder):
    """Probe a single folder for processable videos (non-recursive).

    Runs ffprobe, checks claims, checks v2 tags, and decides strategy
    for each file. Returns (work_items, skipped_claimed_count).
    """
    work_items = []
    skipped_claimed = 0

    for path in sorted(folder.iterdir()):
        if not path.is_file() or path.suffix.lower() not in VIDEO_EXTENSIONS:
            continue
        if not is_processable(path):
            continue

        # Skip already-claimed files
        if claim.is_claimed(str(path)):
            skipped_claimed += 1
            continue

        info = encode.ffprobe_info(path)
        if info is None:
            continue

        # Skip files already encoded with current H.264 settings
        comment = info.get('comment') or ''
        if encode.ENCODER_TAG in comment:  # compressed_h264_v4
            continue

        ext = path.suffix.lower()
        decision = strategy.decide_action(
            info['codec'], info['bitrate'], info['width'], info['height'],
            has_v2_tag=False, ext=ext,
        )

        try:
            size = path.stat().st_size
        except OSError:
            continue

        work_items.append({
            'path': path,
            'size': size,
            'info': info,
            'decision': decision,
            'ext': ext,
        })

    # Sort by size descending within folder
    work_items.sort(key=lambda w: w['size'], reverse=True)
    return work_items, skipped_claimed


def _process_work_item(w, stats, total_saved_bytes):
    """Process a single work item. Returns updated total_saved_bytes."""
    path = w['path']
    decision = w['decision']
    action = decision['action']

    print(f'  {human_size(w["size"])}  {w["info"]["codec"]}  {action}  {decision["reason"]}')

    # ── Skip actions: just rename ──
    if action.startswith('skip_'):
        new_path = encode.rename_skip(path)
        if new_path:
            print(f'  SKIP -> {Path(new_path).name}')
            stats['skipped'] += 1
        else:
            print(f'  SKIP: rename failed')
            stats['failed'] += 1
        return total_saved_bytes

    # ── Remux / Encode: need to claim first ──
    if not claim.claim_file(str(path)):
        print(f'  SKIP: already claimed by another machine')
        stats['already_claimed'] += 1
        return total_saved_bytes

    # Verify file still exists
    if not path.exists():
        print(f'  SKIP: file no longer exists')
        claim.release_claim(str(path))
        stats['failed'] += 1
        return total_saved_bytes

    # Re-check tag after claiming — another machine may have just finished this file
    recheck = encode.ffprobe_info(path)
    if recheck and encode.ENCODER_TAG in (recheck.get('comment') or ''):
        print(f'  SKIP: already encoded by another machine')
        claim.release_claim(str(path))
        return total_saved_bytes

    if action == 'remux':
        # Remux: copy streams into MP4
        t0 = time.time()
        result = encode.remux_to_mp4(path)
        remux_time = time.time() - t0

        if not result['success']:
            print(f'  FAILED: {result["error"]}')
            claim.release_claim(str(path))
            stats['failed'] += 1
            return total_saved_bytes

        # Backup original (non-MP4 file)
        backup_path = encode.move_to_backup(path)
        if backup_path is None:
            print(f'  FAILED: could not backup original')
            encode._cleanup(result['output_path'])
            claim.release_claim(str(path))
            stats['failed'] += 1
            return total_saved_bytes

        print(f'  REMUXED: {human_size(w["size"])} -> {human_size(result["output_size"])} '
              f'({remux_time:.0f}s)')
        stats['remuxed'] += 1
        claim.release_claim(str(path))
        return total_saved_bytes

    # ── Encode ──
    scale_filter = None
    if decision['downscale_1080p']:
        scale_filter = strategy.get_scale_filter(w['info']['width'], w['info']['height'])

    use_size_gate = decision.get('size_gate', True)

    t0 = time.time()
    result = encode.encode_video(
        path,
        source_codec=w['info']['codec'],
        target_cq=decision['target_cq'],
        scale_filter=scale_filter,
        size_gate=use_size_gate,
    )
    encode_time = time.time() - t0

    if not result['success']:
        print(f'  FAILED: {result["error"]}')
        claim.release_claim(str(path))
        stats['failed'] += 1
        return total_saved_bytes

    if not result.get('passed_size_gate'):
        # Only reachable when size_gate=True (MP4 sources)
        print(f'  NO SAVINGS: {result["savings_pct"]:.1f}% (need {encode.MIN_SAVINGS_PCT}%)')
        new_path = encode.rename_skip(path)
        if new_path:
            print(f'  -> {Path(new_path).name}')
        stats['skip_no_savings'] += 1
        claim.release_claim(str(path))
        return total_saved_bytes

    # Backup original
    backup_path = encode.move_to_backup(path)
    if backup_path is None:
        print(f'  FAILED: could not backup original')
        encode._cleanup(result['temp_path'])
        claim.release_claim(str(path))
        stats['failed'] += 1
        return total_saved_bytes

    # Finalize: rename temp -> final
    if not encode.finalize(result['temp_path'], result['final_path']):
        print(f'  FAILED: could not rename temp file')
        claim.release_claim(str(path))
        stats['failed'] += 1
        return total_saved_bytes

    saved = w['size'] - result['output_size']
    total_saved_bytes += saved
    print(f'  OK: {human_size(w["size"])} -> {human_size(result["output_size"])} '
          f'({result["savings_pct"]:.1f}% saved, {encode_time:.0f}s)')
    stats['compressed'] += 1
    claim.release_claim(str(path))
    return total_saved_bytes


# ─── run ─────────────────────────────────────────────────────────────────────

def cmd_run(args):
    """Discover folders, then probe and process each folder on demand."""
    folders = [Path(f).resolve() for f in args.folders]
    for folder in folders:
        if not folder.is_dir():
            print(f'Error: {folder} is not a directory')
            sys.exit(1)

    dry_run = args.dry_run
    batch = args.batch

    # Optionally recover stale claims
    if args.recover_stale and not dry_run:
        recovered = claim.recover_stale(dry_run=False)
        if recovered:
            print(f'Recovered {len(recovered)} stale claims')
            for cp, data in recovered:
                print(f'  {data.get("video_path", cp)}')
            print()

    # Phase 1: Discover folders with processable videos (fast, no ffprobe)
    print('Scanning for folders with video files...')
    target_folders = discover_folders(folders)
    print(f'Found {len(target_folders)} folders with processable videos')

    if not target_folders:
        print('Nothing to process.')
        return

    print(f'\nMachine: {HOSTNAME}')

    stats = {'compressed': 0, 'remuxed': 0, 'skipped': 0, 'failed': 0, 'already_claimed': 0,
             'skip_no_savings': 0}
    total_saved_bytes = 0
    total_processed = 0
    start_time = time.time()

    # Phase 2: Process folder by folder (probe each folder just before processing)
    for fi, folder in enumerate(target_folders, 1):
        if batch and total_processed >= batch:
            break

        print(f'\n--- Folder {fi}/{len(target_folders)}: {folder} ---')
        print('  Probing files...')
        work_items, skipped_claimed = scan_folder(folder)

        if skipped_claimed:
            print(f'  {skipped_claimed} files already claimed, skipping')

        if not work_items:
            print('  No files to process in this folder.')
            continue

        # Apply remaining batch limit
        if batch:
            remaining = batch - total_processed
            work_items = work_items[:remaining]

        # Summarize folder
        action_counts = {}
        folder_size = 0
        for w in work_items:
            action = w['decision']['action']
            action_counts[action] = action_counts.get(action, 0) + 1
            folder_size += w['size']

        print(f'  {len(work_items)} files ({human_size(folder_size)}):')
        for action, cnt in sorted(action_counts.items(), key=lambda x: -x[1]):
            print(f'    {action:<15s} {cnt:>6d}')

        if dry_run:
            for w in work_items[:50]:
                d = w['decision']
                ds = ' [4K->1080p]' if d['downscale_1080p'] else ''
                sg = ' [size-gate]' if d.get('size_gate') else ''
                cq = f' CQ{d["target_cq"]}' if d['target_cq'] else ''
                print(f'    {d["action"]:<8s}{cq}{ds}{sg}  {human_size(w["size"]):>12s}  {w["path"].name}')
                print(f'      {d["reason"]}')
            if len(work_items) > 50:
                print(f'    ... and {len(work_items) - 50} more')
            total_processed += len(work_items)
            continue

        # Process files in this folder
        for i, w in enumerate(work_items, 1):
            total_processed += 1
            elapsed = time.time() - start_time
            done = stats['compressed'] + stats['remuxed']
            rate = done / elapsed * 3600 if elapsed > 0 and done > 0 else 0

            print(f'  [{i}/{len(work_items)}] {w["path"]}')
            total_saved_bytes = _process_work_item(w, stats, total_saved_bytes)

            # Running totals after encode/remux
            done = stats['compressed'] + stats['remuxed']
            if done > 0:
                print(f'  Running: {stats["compressed"]} encoded, {stats["remuxed"]} remuxed, '
                      f'{stats["skipped"]} skipped, {stats["skip_no_savings"]} no-savings, '
                      f'{stats["failed"]} failed | saved {human_size(total_saved_bytes)}')
                if rate > 0:
                    print(f'  Rate: {rate:.1f} files/hr')

    # Final summary
    elapsed = time.time() - start_time
    print(f'\n{"="*60}')
    print(f'RUN COMPLETE ({HOSTNAME})')
    print(f'{"="*60}')
    print(f'  Folders scanned: {len(target_folders)}')
    print(f'  Encoded:         {stats["compressed"]}')
    print(f'  Remuxed:         {stats["remuxed"]}')
    print(f'  Skipped:         {stats["skipped"]}')
    print(f'  No savings:      {stats["skip_no_savings"]}')
    print(f'  Failed:          {stats["failed"]}')
    print(f'  Already claimed: {stats["already_claimed"]}')
    print(f'  Total saved:     {human_size(total_saved_bytes)}')
    print(f'  Elapsed:         {elapsed/60:.1f} min')


# ─── status ──────────────────────────────────────────────────────────────────

def cmd_status(args):
    """Walk filesystem and count files by state."""
    folders = [Path(f).resolve() for f in args.folders]
    for folder in folders:
        if not folder.is_dir():
            print(f'Error: {folder} is not a directory')
            sys.exit(1)

    counts = {'compressed': 0, 'skip': 0, 'remaining': 0, 'claimed': 0}
    sizes = {'compressed': 0, 'skip': 0, 'remaining': 0}

    for folder in folders:
        for path in folder.rglob('*'):
            if not path.is_file() or path.suffix.lower() not in VIDEO_EXTENSIONS:
                continue
            name = path.name
            if name.startswith('.') or '.tmp' in name.lower():
                continue

            try:
                size = path.stat().st_size
            except OSError:
                continue

            stem = path.stem.lower()
            if '_skip' in stem:
                counts['skip'] += 1
                sizes['skip'] += size
            elif claim.is_claimed(str(path)):
                counts['claimed'] += 1
            else:
                # Check metadata tag to distinguish compressed from remaining
                info = encode.ffprobe_info(path)
                comment = info.get('comment', '') if info else ''
                if encode.ENCODER_TAG in comment:
                    counts['compressed'] += 1
                    sizes['compressed'] += size
                else:
                    counts['remaining'] += 1
                    sizes['remaining'] += size

    total = sum(counts.values())
    print(f'Video files: {total}')
    print(f'  Compressed:  {counts["compressed"]:>6d}  ({human_size(sizes["compressed"])})')
    print(f'  Skipped:     {counts["skip"]:>6d}  ({human_size(sizes["skip"])})')
    print(f'  In progress: {counts["claimed"]:>6d}')
    print(f'  Remaining:   {counts["remaining"]:>6d}  ({human_size(sizes["remaining"])})')

    # Active claims
    claims = claim.list_claims()
    if claims:
        print(f'\nActive NAS claims: {len(claims)}')
        by_host = {}
        for c in claims:
            h = c.get('hostname', 'unknown')
            by_host[h] = by_host.get(h, 0) + 1
        for h, cnt in sorted(by_host.items()):
            print(f'  {h}: {cnt} files')


# ─── migrate ─────────────────────────────────────────────────────────────────

def cmd_migrate(args):
    """Find v2-compressed files (by metadata tag) and rename to _compressed.mp4."""
    folders = [Path(f).resolve() for f in args.folders]
    for folder in folders:
        if not folder.is_dir():
            print(f'Error: {folder} is not a directory')
            sys.exit(1)

    dry_run = args.dry_run
    found = 0
    renamed = 0
    errors = 0

    for folder in folders:
        for path in folder.rglob('*.mp4'):
            if not path.is_file():
                continue
            name = path.name
            stem = path.stem.lower()
            if name.startswith('.') or '.tmp' in name.lower():
                continue
            # Already has _compressed in name — skip
            if '_compressed' in stem:
                continue

            info = encode.ffprobe_info(path)
            if info is None:
                continue

            comment = info.get('comment', '')
            if encode.ENCODER_TAG_V2 not in comment:
                continue

            found += 1
            new_name = f'{path.stem}_compressed.mp4'
            new_path = path.parent / new_name

            if dry_run:
                print(f'  Would rename: {path.name} -> {new_name}')
                continue

            if new_path.exists():
                print(f'  SKIP: {new_name} already exists')
                errors += 1
                continue

            try:
                path.rename(new_path)
                print(f'  Renamed: {path.name} -> {new_name}')
                renamed += 1
            except Exception as e:
                print(f'  FAILED: {path.name}: {e}')
                errors += 1

    print(f'\nMigrate: {found} v2-tagged files found')
    if dry_run:
        print('(dry run — no changes made)')
    else:
        print(f'  Renamed: {renamed}')
        if errors:
            print(f'  Errors:  {errors}')


# ─── main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Video compression v3 - filesystem-based tracking',
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    # run
    p_run = subparsers.add_parser('run', help='Discover + process video files')
    p_run.add_argument('folders', nargs='+', help='Folders to scan')
    p_run.add_argument('--batch', '-b', type=int, default=None,
                       help='Max files to process (default: all)')
    p_run.add_argument('--dry-run', '-n', action='store_true',
                       help='Show what would happen without processing')
    p_run.add_argument('--recover-stale', action='store_true',
                       help='Release claims older than 24h before starting')
    p_run.set_defaults(func=cmd_run)

    # status
    p_status = subparsers.add_parser('status', help='Show file counts by state')
    p_status.add_argument('folders', nargs='+', help='Folders to check')
    p_status.set_defaults(func=cmd_status)

    # migrate
    p_migrate = subparsers.add_parser('migrate', help='Rename v2-tagged files to _compressed.mp4')
    p_migrate.add_argument('folders', nargs='+', help='Folders to migrate')
    p_migrate.add_argument('--dry-run', '-n', action='store_true',
                           help='Show what would happen without renaming')
    p_migrate.set_defaults(func=cmd_migrate)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
