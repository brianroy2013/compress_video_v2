#!/usr/bin/env python3
"""
Audit Compressions

Scans backed-up originals in BACKUP_BASE, compares each against its
compressed counterpart, and reports which files got bigger, stayed
marginal, or compressed well. Outputs a CSV with full details.
"""

import csv
import json
import subprocess
import sys
from pathlib import Path

BACKUP_BASE = Path('/home/brian/share/orig_video_to_delete')
VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.mpeg', '.mpg'}


def human_size(nbytes):
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if abs(nbytes) < 1024.0:
            return f'{nbytes:.2f} {unit}'
        nbytes /= 1024.0
    return f'{nbytes:.2f} PB'


def ffprobe_info(file: Path) -> dict:
    """Get codec, resolution, bitrate, and duration via ffprobe."""
    cmd = [
        'ffprobe', '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=codec_name,width,height,bit_rate',
        '-show_entries', 'format=duration,bit_rate',
        '-print_format', 'json',
        str(file)
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.returncode != 0:
            return {}
        data = json.loads(r.stdout)
        stream = data.get('streams', [{}])[0] if data.get('streams') else {}
        fmt = data.get('format', {})

        width = stream.get('width', 0) or 0
        height = stream.get('height', 0) or 0

        # Prefer stream bitrate, fall back to format bitrate
        bitrate_str = stream.get('bit_rate') or fmt.get('bit_rate') or '0'
        try:
            bitrate = int(bitrate_str)
        except (ValueError, TypeError):
            bitrate = 0

        try:
            duration = float(fmt.get('duration', 0))
        except (ValueError, TypeError):
            duration = 0.0

        return {
            'codec': stream.get('codec_name', 'unknown'),
            'width': width,
            'height': height,
            'resolution': f'{width}x{height}' if width and height else 'unknown',
            'bitrate': bitrate,
            'duration': duration,
        }
    except Exception:
        return {}


def classify(orig_size, comp_size):
    """Classify compression result."""
    if comp_size >= orig_size:
        return 'bigger'
    ratio = (orig_size - comp_size) / orig_size
    if ratio < 0.05:
        return 'marginal'
    return 'good'


def find_compressed_path(backup_path: Path) -> Path | None:
    """Derive the compressed file path from a backup path.

    Backup stores files as: BACKUP_BASE / absolute_path_without_leading_slash
    The compressed version is at the original location with .mp4 extension.
    """
    rel = backup_path.relative_to(BACKUP_BASE)
    original_location = Path('/') / rel

    # Compressed version is always .mp4
    compressed = original_location.with_suffix('.mp4')
    if compressed.exists():
        return compressed

    # If original was already .mp4, check if it exists as-is
    if original_location.exists():
        return original_location

    return None


def main():
    print(f'Scanning backups in: {BACKUP_BASE}')
    print()

    # Find all backed-up originals
    backup_files = []
    for path in BACKUP_BASE.rglob('*'):
        if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS:
            backup_files.append(path)
    backup_files.sort()

    total = len(backup_files)
    print(f'Found {total} backed-up originals')
    print()

    # Analyze each file
    results = []
    for i, bp in enumerate(backup_files, 1):
        if i % 50 == 0 or i == 1 or i == total:
            print(f'  Probing {i}/{total}...', flush=True)

        orig_size = bp.stat().st_size
        orig_info = ffprobe_info(bp)

        cp = find_compressed_path(bp)

        if cp is None:
            results.append({
                'backup_path': str(bp),
                'compressed_path': '',
                'orig_size': orig_size,
                'comp_size': 0,
                'orig_codec': orig_info.get('codec', 'unknown'),
                'comp_codec': '',
                'orig_resolution': orig_info.get('resolution', 'unknown'),
                'comp_resolution': '',
                'orig_bitrate': orig_info.get('bitrate', 0),
                'comp_bitrate': 0,
                'duration': orig_info.get('duration', 0),
                'size_diff': 0,
                'compression_ratio': 0,
                'classification': 'missing',
            })
            continue

        comp_size = cp.stat().st_size
        comp_info = ffprobe_info(cp)
        size_diff = orig_size - comp_size
        ratio = comp_size / orig_size if orig_size > 0 else 0

        results.append({
            'backup_path': str(bp),
            'compressed_path': str(cp),
            'orig_size': orig_size,
            'comp_size': comp_size,
            'orig_codec': orig_info.get('codec', 'unknown'),
            'comp_codec': comp_info.get('codec', 'unknown'),
            'orig_resolution': orig_info.get('resolution', 'unknown'),
            'comp_resolution': comp_info.get('resolution', 'unknown'),
            'orig_bitrate': orig_info.get('bitrate', 0),
            'comp_bitrate': comp_info.get('bitrate', 0),
            'duration': orig_info.get('duration', 0),
            'size_diff': size_diff,
            'compression_ratio': ratio,
            'classification': classify(orig_size, comp_size),
        })

    # --- Write CSV ---
    csv_path = Path(__file__).parent / 'audit_results.csv'
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'backup_path', 'compressed_path', 'orig_size', 'comp_size',
            'orig_codec', 'comp_codec', 'orig_resolution', 'comp_resolution',
            'orig_bitrate', 'comp_bitrate', 'duration',
            'size_diff', 'compression_ratio', 'classification',
        ])
        writer.writeheader()
        writer.writerows(results)
    print(f'\nCSV written to: {csv_path}')

    # --- Console Report ---
    missing = [r for r in results if r['classification'] == 'missing']
    bigger = [r for r in results if r['classification'] == 'bigger']
    marginal = [r for r in results if r['classification'] == 'marginal']
    good = [r for r in results if r['classification'] == 'good']

    print()
    print('=' * 80)
    print('COMPRESSION AUDIT SUMMARY')
    print('=' * 80)
    print(f'  Total files:     {total}')
    print(f'  Missing:         {len(missing)}')
    print(f'  Bigger:          {len(bigger)}')
    print(f'  Marginal (<5%):  {len(marginal)}')
    print(f'  Good (>=5%):     {len(good)}')
    print()

    # Size stats
    total_orig = sum(r['orig_size'] for r in results if r['classification'] != 'missing')
    total_comp = sum(r['comp_size'] for r in results if r['classification'] != 'missing')
    total_saved = total_orig - total_comp
    wasted_by_bigger = sum(-r['size_diff'] for r in bigger)

    print(f'  Total original size:   {human_size(total_orig)}')
    print(f'  Total compressed size: {human_size(total_comp)}')
    print(f'  Net space saved:       {human_size(total_saved)}')
    print(f'  Space wasted (bigger): {human_size(wasted_by_bigger)} across {len(bigger)} files')
    print()

    # --- Worst offenders (top 20 biggest size increases) ---
    if bigger:
        print('-' * 80)
        print('TOP 20 WORST OFFENDERS (biggest size increases)')
        print('-' * 80)
        worst = sorted(bigger, key=lambda r: r['size_diff'])[:20]
        for r in worst:
            name = Path(r['backup_path']).name
            increase = -r['size_diff']
            pct = (increase / r['orig_size'] * 100) if r['orig_size'] > 0 else 0
            print(f'  +{human_size(increase):>10s} (+{pct:.1f}%)  {r["orig_codec"]:>5s} {r["orig_resolution"]:>10s}  {name}')
        print()

    # --- Breakdown by source codec ---
    print('-' * 80)
    print('BREAKDOWN BY SOURCE CODEC')
    print('-' * 80)
    codecs = sorted(set(r['orig_codec'] for r in results if r['classification'] != 'missing'))
    for codec in codecs:
        codec_results = [r for r in results if r['orig_codec'] == codec and r['classification'] != 'missing']
        if not codec_results:
            continue
        c_bigger = [r for r in codec_results if r['classification'] == 'bigger']
        c_marginal = [r for r in codec_results if r['classification'] == 'marginal']
        c_good = [r for r in codec_results if r['classification'] == 'good']
        c_orig = sum(r['orig_size'] for r in codec_results)
        c_comp = sum(r['comp_size'] for r in codec_results)
        c_saved = c_orig - c_comp
        avg_ratio = (c_comp / c_orig) if c_orig > 0 else 0

        print(f'  {codec.upper():>6s}:  {len(codec_results):>3d} files | '
              f'{len(c_good)} good, {len(c_marginal)} marginal, {len(c_bigger)} bigger | '
              f'avg ratio {avg_ratio:.2f} | net {human_size(c_saved)}')

    # --- Breakdown by resolution ---
    print()
    print('-' * 80)
    print('BREAKDOWN BY RESOLUTION')
    print('-' * 80)
    non_missing = [r for r in results if r['classification'] != 'missing']

    def resolution_bucket(res_str):
        try:
            h = int(res_str.split('x')[1])
        except (IndexError, ValueError):
            return 'unknown'
        if h <= 480:
            return '480p'
        if h <= 720:
            return '720p'
        if h <= 1080:
            return '1080p'
        if h <= 1440:
            return '1440p'
        return '4K+'

    buckets = sorted(set(resolution_bucket(r['orig_resolution']) for r in non_missing))
    for bucket in buckets:
        br = [r for r in non_missing if resolution_bucket(r['orig_resolution']) == bucket]
        if not br:
            continue
        b_bigger = [r for r in br if r['classification'] == 'bigger']
        b_orig = sum(r['orig_size'] for r in br)
        b_comp = sum(r['comp_size'] for r in br)
        b_saved = b_orig - b_comp
        avg_ratio = (b_comp / b_orig) if b_orig > 0 else 0

        print(f'  {bucket:>7s}:  {len(br):>3d} files | '
              f'{len(b_bigger)} bigger | '
              f'avg ratio {avg_ratio:.2f} | net {human_size(b_saved)}')

    print()
    print('=' * 80)


if __name__ == '__main__':
    main()
