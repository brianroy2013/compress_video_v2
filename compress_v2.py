#!/usr/bin/env python3
"""
Video Compression v2 - Database-backed multi-machine system.

Subcommands:
    scan [folders...]   Discover files, probe with ffprobe, populate DB
    plan                Dry run: show what would be done
    run [--batch N]     Claim and encode files
    status              Progress report
    import              Bootstrap DB from video_inventory.json + audit_results.csv
    fix-audit           Restore badly-compressed files from backup
"""

import argparse
import csv
import json
import socket
import sys
import time
from datetime import datetime
from pathlib import Path

import claim
import db
import encode
import strategy

VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.mpeg', '.mpg'}
INVENTORY_FILE = Path(__file__).parent / 'video_inventory.json'
AUDIT_CSV = Path(__file__).parent / 'audit_results.csv'
HOSTNAME = socket.gethostname()


def human_size(nbytes):
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if abs(nbytes) < 1024.0:
            return f'{nbytes:.2f} {unit}'
        nbytes /= 1024.0
    return f'{nbytes:.2f} PB'


# ─── scan ────────────────────────────────────────────────────────────────────

def cmd_scan(args):
    """Discover video files, probe them, populate DB with strategy decisions."""
    conn = db.get_connection()
    db.init_db(conn)

    folders = [Path(f).resolve() for f in args.folders]
    for folder in folders:
        if not folder.is_dir():
            print(f'Error: {folder} is not a directory')
            sys.exit(1)

    # Find all video files
    all_files = []
    for folder in folders:
        print(f'Scanning {folder}...')
        for path in folder.rglob('*'):
            if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS:
                all_files.append(path)
    all_files.sort()

    total = len(all_files)
    print(f'Found {total} video files')

    # Check which are already in DB
    existing = set()
    for row in conn.execute('SELECT path FROM videos').fetchall():
        existing.add(row['path'])

    new_files = [f for f in all_files if str(f) not in existing]
    print(f'  {len(new_files)} new files to probe ({len(existing)} already in DB)')

    if not new_files:
        print('Nothing new to scan.')
        return

    # Probe and insert
    scanned = 0
    failed = 0
    now = datetime.now().isoformat()

    for i, filepath in enumerate(new_files, 1):
        if i % 100 == 0 or i == 1 or i == len(new_files):
            print(f'  Probing {i}/{len(new_files)}...', flush=True)

        info = encode.ffprobe_info(filepath)
        if info is None:
            failed += 1
            continue

        has_tag = encode.ENCODER_TAG in (info.get('comment') or '')
        decision = strategy.decide_action(
            info['codec'], info['bitrate'], info['width'], info['height'],
            has_v2_tag=has_tag,
        )

        try:
            size = filepath.stat().st_size
        except OSError:
            failed += 1
            continue

        db.upsert_video(
            conn,
            path=str(filepath),
            filename=filepath.name,
            size_bytes=size,
            codec=info['codec'],
            width=info['width'],
            height=info['height'],
            bitrate=info['bitrate'],
            duration_sec=info['duration'],
            action=decision['action'],
            target_cq=decision['target_cq'],
            downscale_1080p=1 if decision['downscale_1080p'] else 0,
            status='pending' if decision['action'] == 'encode' else 'skipped',
            scanned_at=now,
        )
        scanned += 1

    print(f'\nScan complete: {scanned} added, {failed} failed to probe')
    _print_action_summary(conn)


# ─── plan ────────────────────────────────────────────────────────────────────

def cmd_plan(args):
    """Dry run: show what the run command would do."""
    conn = db.get_connection()
    db.init_db(conn)

    _print_action_summary(conn)

    # Show pending encodes
    pending = db.get_pending_encodes(conn)
    if not pending:
        print('\nNo pending encodes.')
        return

    print(f'\nPending encodes: {len(pending)} files')
    total_size = sum(v['size_bytes'] for v in pending)
    print(f'Total input size: {human_size(total_size)}')

    # Show breakdown by action details
    cq_groups = {}
    downscale_count = 0
    for v in pending:
        cq = v['target_cq']
        cq_groups[cq] = cq_groups.get(cq, 0) + 1
        if v['downscale_1080p']:
            downscale_count += 1

    print(f'\nBy CQ value:')
    for cq in sorted(cq_groups):
        print(f'  CQ {cq}: {cq_groups[cq]} files')
    print(f'  4K downscale: {downscale_count} files')

    # Show first few
    if args.verbose:
        print(f'\nFirst 50 (largest first):')
        for v in pending[:50]:
            ds = ' [4K->1080p]' if v['downscale_1080p'] else ''
            print(f'  {human_size(v["size_bytes"]):>12s}  CQ{v["target_cq"]}  {v["codec"]:>5s}{ds}  {v["filename"]}')


# ─── run ─────────────────────────────────────────────────────────────────────

def cmd_run(args):
    """Claim and encode files."""
    conn = db.get_connection()
    db.init_db(conn)

    batch = args.batch
    recover_stale = args.recover_stale

    # Optionally recover stale claims first
    if recover_stale:
        recovered = claim.recover_stale(dry_run=False)
        if recovered:
            print(f'Recovered {len(recovered)} stale claims')
            for cp, data in recovered:
                print(f'  {data.get("video_path", cp)}')

    pending = db.get_pending_encodes(conn)
    if not pending:
        print('No pending encodes.')
        return

    total_available = len(pending)
    to_process = pending[:batch] if batch else pending
    print(f'{total_available} pending encodes, processing up to {len(to_process)}')
    print(f'Machine: {HOSTNAME}\n')

    stats = {'encoded': 0, 'skipped_no_savings': 0, 'failed': 0, 'already_claimed': 0}
    total_saved_bytes = 0
    start_time = time.time()

    for i, video in enumerate(to_process, 1):
        vid = video['id']
        vpath = video['path']
        elapsed = time.time() - start_time
        rate = stats['encoded'] / elapsed * 3600 if elapsed > 0 and stats['encoded'] > 0 else 0

        print(f'[{i}/{len(to_process)}] {video["filename"]}')
        print(f'  {human_size(video["size_bytes"])}  {video["codec"]}  CQ{video["target_cq"]}'
              f'{"  4K->1080p" if video["downscale_1080p"] else ""}')

        # Try to claim on NAS
        if not claim.claim_file(vpath):
            print(f'  SKIP: already claimed by another machine')
            stats['already_claimed'] += 1
            continue

        # Update DB status
        db.update_status(conn, vid, 'claimed',
                         claimed_by=HOSTNAME,
                         claimed_at=datetime.now().isoformat())
        db.log_event(conn, vid, HOSTNAME, 'claimed')

        # Verify file still exists
        filepath = Path(vpath)
        if not filepath.exists():
            print(f'  SKIP: file no longer exists')
            db.update_status(conn, vid, 'failed', error_message='File not found')
            db.log_event(conn, vid, HOSTNAME, 'failed', 'File not found')
            claim.release_claim(vpath)
            stats['failed'] += 1
            continue

        # Build scale filter if needed
        scale_filter = None
        if video['downscale_1080p']:
            scale_filter = strategy.get_scale_filter(video['width'], video['height'])

        # Encode
        db.update_status(conn, vid, 'encoding')
        db.log_event(conn, vid, HOSTNAME, 'encoding_started')

        t0 = time.time()
        result = encode.encode_video(
            vpath,
            source_codec=video['codec'],
            target_cq=video['target_cq'],
            scale_filter=scale_filter,
        )
        encode_time = time.time() - t0

        if not result['success']:
            print(f'  FAILED: {result["error"]}')
            db.update_status(conn, vid, 'failed', error_message=result['error'])
            db.log_event(conn, vid, HOSTNAME, 'failed', result['error'])
            claim.release_claim(vpath)
            stats['failed'] += 1
            continue

        if not result.get('passed_size_gate'):
            print(f'  NO SAVINGS: {result["savings_pct"]:.1f}% (need {encode.MIN_SAVINGS_PCT}%)')
            db.update_status(conn, vid, 'skipped_no_savings',
                             output_size=result['output_size'],
                             savings_pct=result['savings_pct'])
            db.log_event(conn, vid, HOSTNAME, 'skipped_no_savings',
                         f'{result["savings_pct"]:.1f}%')
            # Don't release claim -- we've already decided this file, no point retrying
            stats['skipped_no_savings'] += 1
            continue

        # Backup original
        backup_path = encode.move_to_backup(vpath)
        if backup_path is None:
            print(f'  FAILED: could not backup original')
            encode._cleanup(result['temp_path'])
            db.update_status(conn, vid, 'failed', error_message='Backup failed')
            db.log_event(conn, vid, HOSTNAME, 'failed', 'Backup move failed')
            claim.release_claim(vpath)
            stats['failed'] += 1
            continue

        # Finalize: rename temp -> final
        if not encode.finalize(result['temp_path'], result['final_path']):
            print(f'  FAILED: could not rename temp file')
            db.update_status(conn, vid, 'failed', error_message='Rename failed')
            db.log_event(conn, vid, HOSTNAME, 'failed', 'Temp rename failed')
            claim.release_claim(vpath)
            stats['failed'] += 1
            continue

        # Success!
        saved = video['size_bytes'] - result['output_size']
        total_saved_bytes += saved
        print(f'  OK: {human_size(video["size_bytes"])} -> {human_size(result["output_size"])} '
              f'({result["savings_pct"]:.1f}% saved, {encode_time:.0f}s)')

        db.update_status(conn, vid, 'compressed',
                         output_path=result['final_path'],
                         output_size=result['output_size'],
                         savings_pct=result['savings_pct'],
                         backup_path=backup_path,
                         finished_at=datetime.now().isoformat())
        db.log_event(conn, vid, HOSTNAME, 'compressed',
                     f'{result["savings_pct"]:.1f}% saved in {encode_time:.0f}s')
        stats['encoded'] += 1

        # Running totals
        print(f'  Running: {stats["encoded"]} encoded, {stats["skipped_no_savings"]} no-savings, '
              f'{stats["failed"]} failed, {stats["already_claimed"]} claimed | '
              f'saved {human_size(total_saved_bytes)}')
        if rate > 0:
            print(f'  Rate: {rate:.1f} files/hr')

    # Final summary
    elapsed = time.time() - start_time
    print(f'\n{"="*60}')
    print(f'RUN COMPLETE ({HOSTNAME})')
    print(f'{"="*60}')
    print(f'  Encoded:         {stats["encoded"]}')
    print(f'  No savings:      {stats["skipped_no_savings"]}')
    print(f'  Failed:          {stats["failed"]}')
    print(f'  Already claimed: {stats["already_claimed"]}')
    print(f'  Total saved:     {human_size(total_saved_bytes)}')
    print(f'  Elapsed:         {elapsed/60:.1f} min')


# ─── status ──────────────────────────────────────────────────────────────────

def cmd_status(args):
    """Show progress report."""
    conn = db.get_connection()
    db.init_db(conn)

    total = conn.execute('SELECT COUNT(*) as n FROM videos').fetchone()['n']
    if total == 0:
        print('Database is empty. Run "import" or "scan" first.')
        return

    print(f'Total videos in DB: {total}\n')

    # By status
    print('By status:')
    status_counts = db.count_by_status(conn)
    for status in ('pending', 'skipped', 'claimed', 'encoding', 'compressed',
                   'skipped_no_savings', 'failed', 'needs_remediation', 'done'):
        cnt = status_counts.get(status, 0)
        if cnt > 0:
            print(f'  {status:<25s} {cnt:>6d}')

    # By action
    print('\nBy action:')
    action_counts = db.count_by_action(conn)
    for action, cnt in sorted(action_counts.items(), key=lambda x: -x[1]):
        print(f'  {action:<25s} {cnt:>6d}')

    # Space savings
    row = conn.execute('''
        SELECT COUNT(*) as n,
               SUM(size_bytes) as orig_total,
               SUM(output_size) as comp_total
        FROM videos WHERE status = 'compressed'
    ''').fetchone()
    if row['n'] > 0:
        saved = row['orig_total'] - row['comp_total']
        pct = saved / row['orig_total'] * 100 if row['orig_total'] > 0 else 0
        print(f'\nCompression results ({row["n"]} files):')
        print(f'  Original:   {human_size(row["orig_total"])}')
        print(f'  Compressed: {human_size(row["comp_total"])}')
        print(f'  Saved:      {human_size(saved)} ({pct:.1f}%)')

    # Active claims on NAS
    claims = claim.list_claims()
    if claims:
        print(f'\nActive NAS claims: {len(claims)}')
        by_host = {}
        for c in claims:
            h = c.get('hostname', 'unknown')
            by_host[h] = by_host.get(h, 0) + 1
        for h, cnt in sorted(by_host.items()):
            print(f'  {h}: {cnt} files')

    # Recent log entries
    if args.verbose:
        rows = conn.execute('''
            SELECT l.timestamp, l.machine, l.event, l.details, v.filename
            FROM processing_log l
            JOIN videos v ON l.video_id = v.id
            ORDER BY l.timestamp DESC LIMIT 20
        ''').fetchall()
        if rows:
            print(f'\nRecent activity (last 20):')
            for r in rows:
                name = r['filename'][:40]
                print(f'  {r["timestamp"]}  {r["machine"]:<12s}  {r["event"]:<20s}  {name}')


# ─── import ──────────────────────────────────────────────────────────────────

def cmd_import(args):
    """Bootstrap DB from video_inventory.json and audit_results.csv."""
    conn = db.get_connection()
    db.init_db(conn)

    now = datetime.now().isoformat()

    # --- Load inventory ---
    if not INVENTORY_FILE.exists():
        print(f'Error: {INVENTORY_FILE} not found')
        sys.exit(1)

    print(f'Loading {INVENTORY_FILE}...')
    with open(INVENTORY_FILE) as f:
        inventory = json.load(f)
    print(f'  {len(inventory)} entries')

    # Build set of already-processed paths from audit
    audit_map = {}  # compressed_path -> classification
    audit_backup_map = {}  # compressed_path -> backup_path
    if AUDIT_CSV.exists():
        print(f'Loading {AUDIT_CSV}...')
        with open(AUDIT_CSV) as f:
            reader = csv.DictReader(f)
            for row in reader:
                cp = row['compressed_path']
                if cp:
                    audit_map[cp] = row['classification']
                    audit_backup_map[cp] = row['backup_path']
        print(f'  {len(audit_map)} audit entries')

    # Insert inventory into DB
    inserted = 0
    skipped = 0
    for entry in inventory:
        vpath = entry['path']
        codec = entry.get('codec', 'unknown')
        width = entry.get('width', 0)
        height = entry.get('height', 0)
        bitrate = entry.get('bitrate', 0)
        duration = entry.get('duration', 0)
        size = entry.get('size', 0)
        filename = Path(vpath).name

        # Decide strategy
        decision = strategy.decide_action(codec, bitrate, width, height)

        # Check if this was already processed in the audit
        audit_class = audit_map.get(vpath)
        if audit_class == 'good':
            status = 'done'
        elif audit_class in ('bigger', 'marginal'):
            status = 'needs_remediation'
        elif audit_class == 'missing':
            status = 'failed'
        elif decision['action'] == 'encode':
            status = 'pending'
        else:
            status = 'skipped'

        db.upsert_video(
            conn,
            path=vpath,
            filename=filename,
            size_bytes=size,
            codec=codec,
            width=width,
            height=height,
            bitrate=bitrate,
            duration_sec=duration,
            action=decision['action'],
            target_cq=decision['target_cq'],
            downscale_1080p=1 if decision['downscale_1080p'] else 0,
            status=status,
            scanned_at=now,
        )

        # Store backup path for audit entries
        if audit_class in ('bigger', 'marginal', 'good'):
            backup_path = audit_backup_map.get(vpath)
            if backup_path:
                vid_row = db.get_video_by_path(conn, vpath)
                if vid_row:
                    db.update_status(conn, vid_row['id'], status,
                                     backup_path=backup_path)

        inserted += 1

    print(f'\nImported {inserted} videos')
    _print_action_summary(conn)

    # Show audit integration stats
    good = sum(1 for c in audit_map.values() if c == 'good')
    bigger = sum(1 for c in audit_map.values() if c == 'bigger')
    marginal = sum(1 for c in audit_map.values() if c == 'marginal')
    missing = sum(1 for c in audit_map.values() if c == 'missing')
    print(f'\nAudit integration:')
    print(f'  Good (done):               {good}')
    print(f'  Bigger (needs remediation): {bigger}')
    print(f'  Marginal (needs remediation): {marginal}')
    print(f'  Missing (failed):          {missing}')


# ─── fix-audit ───────────────────────────────────────────────────────────────

def cmd_fix_audit(args):
    """Restore badly-compressed files from backup and re-evaluate."""
    conn = db.get_connection()
    db.init_db(conn)

    remediation = db.get_needs_remediation(conn)
    if not remediation:
        print('No files need remediation.')
        return

    total = len(remediation)
    print(f'{total} files need remediation')

    if args.dry_run:
        print('\n=== DRY RUN ===')
        for v in remediation:
            bp = v['backup_path']
            exists = Path(bp).exists() if bp else False
            status = 'OK' if exists else 'MISSING backup'
            print(f'  [{status}] {v["filename"]}')
            if bp:
                print(f'    backup: {bp}')
        return

    stats = {'restored': 0, 'backup_missing': 0, 'failed': 0}

    for i, video in enumerate(remediation, 1):
        vid = video['id']
        vpath = video['path']
        backup = video['backup_path']
        print(f'\n[{i}/{total}] {video["filename"]}')

        if not backup:
            # Try to derive backup path
            backup = str(encode.get_backup_path(vpath))

        backup_path = Path(backup)
        if not backup_path.exists():
            print(f'  SKIP: backup not found at {backup}')
            db.log_event(conn, vid, HOSTNAME, 'remediation_skip', 'Backup not found')
            stats['backup_missing'] += 1
            continue

        compressed_path = Path(vpath)

        # Step 1: Delete the bad compressed version
        if compressed_path.exists():
            try:
                compressed_path.unlink()
                print(f'  Deleted bad compressed file')
            except Exception as e:
                print(f'  FAILED to delete compressed file: {e}')
                db.log_event(conn, vid, HOSTNAME, 'remediation_failed', str(e))
                stats['failed'] += 1
                continue

        # Step 2: Restore original from backup
        try:
            import shutil
            compressed_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(backup_path), str(compressed_path))
            print(f'  Restored original from backup')
        except Exception as e:
            print(f'  FAILED to restore: {e}')
            db.log_event(conn, vid, HOSTNAME, 'remediation_failed', str(e))
            stats['failed'] += 1
            continue

        # Step 3: Re-probe and decide strategy
        info = encode.ffprobe_info(compressed_path)
        if info is None:
            print(f'  WARNING: could not probe restored file')
            db.update_status(conn, vid, 'pending')
            stats['restored'] += 1
            continue

        try:
            new_size = compressed_path.stat().st_size
        except OSError:
            new_size = video['size_bytes']

        decision = strategy.decide_action(
            info['codec'], info['bitrate'], info['width'], info['height']
        )

        new_status = 'pending' if decision['action'] == 'encode' else 'skipped'
        db.update_status(conn, vid, new_status,
                         action=decision['action'],
                         target_cq=decision['target_cq'],
                         downscale_1080p=1 if decision['downscale_1080p'] else 0,
                         codec=info['codec'],
                         width=info['width'],
                         height=info['height'],
                         bitrate=info['bitrate'],
                         duration_sec=info['duration'],
                         size_bytes=new_size,
                         backup_path=None,
                         output_path=None,
                         output_size=None,
                         savings_pct=None,
                         error_message=None)
        db.log_event(conn, vid, HOSTNAME, 'remediated',
                     f'{decision["action"]} (was needs_remediation)')

        print(f'  -> {new_status}: {decision["reason"]}')
        stats['restored'] += 1

    print(f'\n{"="*60}')
    print(f'REMEDIATION COMPLETE')
    print(f'{"="*60}')
    print(f'  Restored:       {stats["restored"]}')
    print(f'  Backup missing: {stats["backup_missing"]}')
    print(f'  Failed:         {stats["failed"]}')


# ─── helpers ─────────────────────────────────────────────────────────────────

def _print_action_summary(conn):
    """Print a summary of actions in the DB."""
    print(f'\nDB summary:')
    action_counts = db.count_by_action(conn)
    status_counts = db.count_by_status(conn)
    total = sum(action_counts.values())
    print(f'  Total: {total}')
    print(f'  Actions:')
    for action, cnt in sorted(action_counts.items(), key=lambda x: -x[1]):
        print(f'    {action:<25s} {cnt:>6d}')
    print(f'  Statuses:')
    for status, cnt in sorted(status_counts.items(), key=lambda x: -x[1]):
        print(f'    {status:<25s} {cnt:>6d}')


# ─── main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Video compression v2 - database-backed multi-machine system',
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    # scan
    p_scan = subparsers.add_parser('scan', help='Discover and probe video files')
    p_scan.add_argument('folders', nargs='+', help='Folders to scan')
    p_scan.set_defaults(func=cmd_scan)

    # plan
    p_plan = subparsers.add_parser('plan', help='Dry run: show what would be done')
    p_plan.add_argument('-v', '--verbose', action='store_true', help='Show file details')
    p_plan.set_defaults(func=cmd_plan)

    # run
    p_run = subparsers.add_parser('run', help='Claim and encode files')
    p_run.add_argument('--batch', '-b', type=int, default=None,
                       help='Max files to process (default: all)')
    p_run.add_argument('--recover-stale', action='store_true',
                       help='Release claims older than 24h before starting')
    p_run.set_defaults(func=cmd_run)

    # status
    p_status = subparsers.add_parser('status', help='Progress report')
    p_status.add_argument('-v', '--verbose', action='store_true', help='Show recent activity')
    p_status.set_defaults(func=cmd_status)

    # import
    p_import = subparsers.add_parser('import', help='Bootstrap DB from inventory + audit')
    p_import.set_defaults(func=cmd_import)

    # fix-audit
    p_fix = subparsers.add_parser('fix-audit', help='Restore badly-compressed files')
    p_fix.add_argument('--dry-run', '-n', action='store_true',
                       help='Show what would be done')
    p_fix.set_defaults(func=cmd_fix_audit)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
