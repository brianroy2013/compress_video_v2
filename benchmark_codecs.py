#!/usr/bin/env python3
"""
Codec Benchmark

Samples 100 random videos from the collection, encodes the first 60 seconds
of each with multiple codec/CQ/resolution settings, and reports compression
ratios and encode times. Fully resumable.

Usage:
    python benchmark_codecs.py [--dry-run] [--fresh] [--seed 42] [--samples 100]
"""

import argparse
import csv
import hashlib
import json
import os
import random
import subprocess
import sys
import time
from pathlib import Path

VIDEO_BASE = Path('/home/brian/share/video_courses')
OUTPUT_BASE = Path('/home/brian/userfiles/claude/compres_video/test_output/benchmark')
INVENTORY_CACHE = Path('/home/brian/userfiles/claude/compres_video/video_inventory.json')
PROGRESS_FILE = Path('/home/brian/userfiles/claude/compres_video/benchmark_progress.json')
RESULTS_CSV = Path('/home/brian/userfiles/claude/compres_video/benchmark_results.csv')

VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.mpeg', '.mpg'}

# GPU decoders by codec
GPU_DECODERS = {
    'h264': 'h264_cuvid',
    'hevc': 'hevc_cuvid',
    'av1': 'av1_cuvid',
}

# Test matrix
TEST_SETTINGS = [
    # (encoder, cq, scale_to_1080p)
    ('hevc_nvenc', 24, False),
    ('hevc_nvenc', 26, False),
    ('hevc_nvenc', 28, False),
    ('hevc_nvenc', 30, False),
    ('hevc_nvenc', 32, False),
    ('av1_nvenc', 26, False),
    ('av1_nvenc', 28, False),
    ('av1_nvenc', 30, False),
    ('av1_nvenc', 32, False),
]

# Additional tests only for 4K sources
TESTS_4K_ONLY = [
    ('hevc_nvenc', 26, True),
    ('av1_nvenc', 28, True),
]


def human_size(nbytes):
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if abs(nbytes) < 1024.0:
            return f'{nbytes:.2f} {unit}'
        nbytes /= 1024.0
    return f'{nbytes:.2f} PB'


def ffprobe_info(file: Path) -> dict | None:
    """Get codec, resolution, bitrate, and duration via ffprobe."""
    cmd = [
        'ffprobe', '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=codec_name,width,height,bit_rate',
        '-show_entries', 'format=duration,bit_rate,size',
        '-print_format', 'json',
        str(file)
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if r.returncode != 0:
            return None
        data = json.loads(r.stdout)
        stream = data.get('streams', [{}])[0] if data.get('streams') else {}
        fmt = data.get('format', {})

        width = stream.get('width', 0) or 0
        height = stream.get('height', 0) or 0

        bitrate_str = stream.get('bit_rate') or fmt.get('bit_rate') or '0'
        try:
            bitrate = int(bitrate_str)
        except (ValueError, TypeError):
            bitrate = 0

        try:
            duration = float(fmt.get('duration', 0))
        except (ValueError, TypeError):
            duration = 0.0

        codec = stream.get('codec_name', 'unknown')

        return {
            'codec': codec,
            'width': width,
            'height': height,
            'bitrate': bitrate,
            'duration': duration,
        }
    except Exception:
        return None


def scan_inventory(fresh=False) -> list[dict]:
    """Scan or load cached video inventory."""
    if not fresh and INVENTORY_CACHE.exists():
        print(f'Loading cached inventory from {INVENTORY_CACHE}')
        with open(INVENTORY_CACHE) as f:
            inventory = json.load(f)
        print(f'  {len(inventory)} videos in cache')
        return inventory

    print(f'Scanning {VIDEO_BASE} for videos (this may take a while)...')
    video_files = []
    for root, dirs, files in os.walk(VIDEO_BASE):
        for name in files:
            if Path(name).suffix.lower() in VIDEO_EXTENSIONS:
                video_files.append(Path(root) / name)
    video_files.sort()
    total = len(video_files)
    print(f'  Found {total} video files, probing...')

    inventory = []
    for i, vf in enumerate(video_files, 1):
        if i % 500 == 0 or i == 1 or i == total:
            print(f'  Probing {i}/{total}...', flush=True)
        info = ffprobe_info(vf)
        if info is None:
            continue
        info['path'] = str(vf)
        info['size'] = vf.stat().st_size
        inventory.append(info)

    # Cache
    INVENTORY_CACHE.parent.mkdir(parents=True, exist_ok=True)
    with open(INVENTORY_CACHE, 'w') as f:
        json.dump(inventory, f)
    print(f'  Cached {len(inventory)} videos to {INVENTORY_CACHE}')
    return inventory


def resolution_bucket(height):
    if height <= 0:
        return 'unknown'
    if height <= 480:
        return '480p'
    if height <= 720:
        return '720p'
    if height <= 1080:
        return '1080p'
    if height <= 1440:
        return '1440p'
    return '4K+'


def bitrate_tier(bitrate):
    if bitrate <= 0:
        return 'unknown'
    mbps = bitrate / 1_000_000
    if mbps < 2:
        return 'low'
    if mbps < 8:
        return 'medium'
    return 'high'


def is_4k(height):
    return height > 1080


def stratified_sample(inventory, n, seed):
    """Sample n videos, stratified by codec x resolution x bitrate tier."""
    # Filter out videos shorter than 60 seconds
    eligible = [v for v in inventory if v['duration'] >= 60]
    print(f'  {len(eligible)} videos >= 60s (of {len(inventory)} total)')

    # Build strata
    strata = {}
    for v in eligible:
        key = (v['codec'], resolution_bucket(v['height']), bitrate_tier(v['bitrate']))
        strata.setdefault(key, []).append(v)

    print(f'  {len(strata)} strata found')

    # Proportional allocation
    rng = random.Random(seed)
    sampled = []
    total_eligible = len(eligible)

    # Calculate how many from each stratum
    allocations = {}
    remaining = n
    for key, vids in sorted(strata.items()):
        # Proportional share, at least 1 if stratum is non-empty and we have budget
        share = max(1, round(len(vids) / total_eligible * n))
        allocations[key] = min(share, len(vids))

    # Adjust to hit exactly n
    total_allocated = sum(allocations.values())
    if total_allocated > n:
        # Trim from largest strata
        for key in sorted(allocations, key=lambda k: -allocations[k]):
            if total_allocated <= n:
                break
            trim = min(allocations[key] - 1, total_allocated - n)
            allocations[key] -= trim
            total_allocated -= trim
    elif total_allocated < n:
        # Add to largest strata that have headroom
        for key in sorted(allocations, key=lambda k: -len(strata[k])):
            if total_allocated >= n:
                break
            headroom = len(strata[key]) - allocations[key]
            add = min(headroom, n - total_allocated)
            allocations[key] += add
            total_allocated += add

    for key, count in allocations.items():
        vids = strata[key]
        chosen = rng.sample(vids, min(count, len(vids)))
        sampled.extend(chosen)

    # Final shuffle
    rng.shuffle(sampled)
    return sampled[:n]


def make_output_dir(video_info):
    """Create a unique output directory for a video's benchmark encodes."""
    path = video_info['path']
    name = Path(path).stem[:40]  # Truncate long names
    h = hashlib.md5(path.encode()).hexdigest()[:8]
    safe_name = ''.join(c if c.isalnum() or c in '-_' else '_' for c in name)
    d = OUTPUT_BASE / f'{h}_{safe_name}'
    d.mkdir(parents=True, exist_ok=True)
    return d


def get_tests_for_video(video_info):
    """Return the list of test settings for a given video."""
    tests = list(TEST_SETTINGS)
    if is_4k(video_info['height']):
        tests.extend(TESTS_4K_ONLY)
    return tests


def encode_key(video_path, encoder, cq, scale):
    """Unique key for a single encode test."""
    return f'{video_path}|{encoder}|cq{cq}|{"1080p" if scale else "native"}'


def run_encode(video_info, encoder, cq, scale_to_1080p, output_dir) -> dict | None:
    """Encode first 60s of a video with given settings. Returns result dict."""
    src = video_info['path']
    codec = video_info['codec']
    scale_label = '1080p' if scale_to_1080p else 'native'
    out_ext = '.mp4'
    out_name = f'{encoder}_cq{cq}_{scale_label}{out_ext}'
    out_path = output_dir / out_name

    cmd = ['ffmpeg', '-y']

    # GPU decoder if available
    gpu_dec = GPU_DECODERS.get(codec)
    if gpu_dec:
        cmd.extend(['-c:v', gpu_dec])

    # Limit to 60 seconds
    cmd.extend(['-t', '60', '-i', src])

    # Video encoder
    if encoder == 'hevc_nvenc':
        cmd.extend(['-c:v', 'hevc_nvenc', '-cq', str(cq), '-preset', 'p5'])
    elif encoder == 'av1_nvenc':
        cmd.extend(['-c:v', 'av1_nvenc', '-cq', str(cq), '-preset', 'p5'])

    # Scale if requested
    if scale_to_1080p:
        cmd.extend(['-vf', 'scale=-2:1080'])

    # Audio
    cmd.extend(['-c:a', 'aac', '-b:a', '128k'])

    cmd.append(str(out_path))

    t0 = time.time()
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        elapsed = time.time() - t0

        if result.returncode != 0:
            print(f'    FAILED: {result.stderr[-200:]}')
            return None

        out_size = out_path.stat().st_size
        # Estimate: ratio is output_size relative to what the same 60s of the
        # source would be (use bitrate * 60 if available, else file size scaled)
        src_60s_size = video_info['bitrate'] * 60 / 8 if video_info['bitrate'] > 0 else (
            video_info['size'] * 60 / video_info['duration'] if video_info['duration'] > 0 else video_info['size']
        )
        ratio = out_size / src_60s_size if src_60s_size > 0 else 0

        return {
            'source_path': src,
            'source_codec': codec,
            'source_width': video_info['width'],
            'source_height': video_info['height'],
            'source_resolution': resolution_bucket(video_info['height']),
            'source_bitrate': video_info['bitrate'],
            'source_bitrate_tier': bitrate_tier(video_info['bitrate']),
            'encoder': encoder,
            'cq': cq,
            'scale': scale_label,
            'output_size': out_size,
            'source_60s_est_size': int(src_60s_size),
            'compression_ratio': round(ratio, 4),
            'encode_time_s': round(elapsed, 1),
            'output_path': str(out_path),
        }
    except subprocess.TimeoutExpired:
        print(f'    TIMEOUT after 600s')
        return None
    except Exception as e:
        print(f'    ERROR: {e}')
        return None


def load_progress():
    """Load progress from disk."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {'completed': {}, 'results': []}


def save_progress(progress):
    """Save progress to disk."""
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)


def write_csv(results):
    """Write results to CSV."""
    if not results:
        return
    RESULTS_CSV.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        'source_path', 'source_codec', 'source_width', 'source_height',
        'source_resolution', 'source_bitrate', 'source_bitrate_tier',
        'encoder', 'cq', 'scale', 'output_size', 'source_60s_est_size',
        'compression_ratio', 'encode_time_s', 'output_path',
    ]
    with open(RESULTS_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)
    print(f'\nCSV written to: {RESULTS_CSV}')


def print_summary(results):
    """Print summary tables."""
    if not results:
        print('No results to summarize.')
        return

    print()
    print('=' * 90)
    print('BENCHMARK RESULTS SUMMARY')
    print('=' * 90)

    # --- Table 1: Average compression ratio by setting ---
    print()
    print('-' * 90)
    print('AVERAGE COMPRESSION RATIO BY SETTING')
    print(f'  {"Encoder":<14s} {"CQ":>4s} {"Scale":>7s} | {"Avg Ratio":>10s} {"Avg Time":>10s} {"Count":>6s}')
    print(f'  {"-"*14} {"-"*4} {"-"*7} | {"-"*10} {"-"*10} {"-"*6}')

    settings_groups = {}
    for r in results:
        key = (r['encoder'], r['cq'], r['scale'])
        settings_groups.setdefault(key, []).append(r)

    for key in sorted(settings_groups.keys()):
        group = settings_groups[key]
        avg_ratio = sum(r['compression_ratio'] for r in group) / len(group)
        avg_time = sum(r['encode_time_s'] for r in group) / len(group)
        encoder, cq, scale = key
        print(f'  {encoder:<14s} {cq:>4d} {scale:>7s} | {avg_ratio:>10.3f} {avg_time:>9.1f}s {len(group):>6d}')

    # --- Table 2: Breakdown by source codec ---
    print()
    print('-' * 90)
    print('BREAKDOWN BY SOURCE CODEC')
    source_codecs = sorted(set(r['source_codec'] for r in results))

    for src_codec in source_codecs:
        codec_results = [r for r in results if r['source_codec'] == src_codec]
        print(f'\n  Source: {src_codec.upper()} ({len(codec_results)} encodes from '
              f'{len(set(r["source_path"] for r in codec_results))} videos)')
        print(f'    {"Encoder":<14s} {"CQ":>4s} {"Scale":>7s} | {"Avg Ratio":>10s} {"Avg Time":>10s} {"Count":>6s}')
        print(f'    {"-"*14} {"-"*4} {"-"*7} | {"-"*10} {"-"*10} {"-"*6}')

        sg = {}
        for r in codec_results:
            key = (r['encoder'], r['cq'], r['scale'])
            sg.setdefault(key, []).append(r)

        for key in sorted(sg.keys()):
            group = sg[key]
            avg_ratio = sum(r['compression_ratio'] for r in group) / len(group)
            avg_time = sum(r['encode_time_s'] for r in group) / len(group)
            encoder, cq, scale = key
            marker = ' <-- BIGGER' if avg_ratio > 1.0 else ''
            print(f'    {encoder:<14s} {cq:>4d} {scale:>7s} | {avg_ratio:>10.3f} {avg_time:>9.1f}s {len(group):>6d}{marker}')

    # --- Table 3: Recommendations ---
    print()
    print('-' * 90)
    print('RECOMMENDATIONS PER SOURCE CODEC')
    print('-' * 90)

    for src_codec in source_codecs:
        codec_results = [r for r in results if r['source_codec'] == src_codec]
        sg = {}
        for r in codec_results:
            key = (r['encoder'], r['cq'], r['scale'])
            sg.setdefault(key, []).append(r)

        # Find best setting (lowest avg ratio that's < 1.0)
        best_key = None
        best_ratio = float('inf')
        for key, group in sg.items():
            avg_ratio = sum(r['compression_ratio'] for r in group) / len(group)
            if avg_ratio < best_ratio:
                best_ratio = avg_ratio
                best_key = key

        if best_key and best_ratio < 1.0:
            encoder, cq, scale = best_key
            savings_pct = (1.0 - best_ratio) * 100
            print(f'  {src_codec.upper():>6s}: {encoder} cq{cq} {scale} -> avg {best_ratio:.3f} ({savings_pct:.1f}% smaller)')
        elif best_key:
            encoder, cq, scale = best_key
            print(f'  {src_codec.upper():>6s}: SKIP re-encoding (best avg ratio {best_ratio:.3f} -- files get bigger)')
        else:
            print(f'  {src_codec.upper():>6s}: No data')

    print()
    print('=' * 90)


def main():
    parser = argparse.ArgumentParser(description='Benchmark codec/CQ settings on sample videos.')
    parser.add_argument('--dry-run', '-n', action='store_true',
                        help='Show what would be done without encoding')
    parser.add_argument('--fresh', action='store_true',
                        help='Ignore cached inventory and progress, start from scratch')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for sampling (default: 42)')
    parser.add_argument('--samples', type=int, default=100,
                        help='Number of videos to sample (default: 100)')
    args = parser.parse_args()

    # --- Load or scan inventory ---
    inventory = scan_inventory(fresh=args.fresh)

    # --- Sample ---
    print(f'\nSelecting {args.samples} stratified samples (seed={args.seed})...')
    sample = stratified_sample(inventory, args.samples, args.seed)
    print(f'  Selected {len(sample)} videos')

    # Show sample distribution
    codec_dist = {}
    res_dist = {}
    for v in sample:
        codec_dist[v['codec']] = codec_dist.get(v['codec'], 0) + 1
        res_dist[resolution_bucket(v['height'])] = res_dist.get(resolution_bucket(v['height']), 0) + 1
    print(f'  Codec distribution: {dict(sorted(codec_dist.items()))}')
    print(f'  Resolution distribution: {dict(sorted(res_dist.items()))}')

    # --- Build test plan ---
    plan = []
    for v in sample:
        tests = get_tests_for_video(v)
        for encoder, cq, scale in tests:
            plan.append((v, encoder, cq, scale))

    total_encodes = len(plan)
    print(f'\n  Total encodes planned: {total_encodes}')

    if args.dry_run:
        print('\n=== DRY RUN ===')
        print(f'Would encode {total_encodes} clips from {len(sample)} videos')
        print(f'Estimated output: ~15-20 GB in {OUTPUT_BASE}')

        # Show a few examples
        print('\nSample plan (first 10):')
        for v, encoder, cq, scale in plan[:10]:
            name = Path(v['path']).name
            scale_label = '1080p' if scale else 'native'
            print(f'  {name[:50]:<50s}  {v["codec"]:>5s} -> {encoder} cq{cq} {scale_label}')
        if total_encodes > 10:
            print(f'  ... and {total_encodes - 10} more')
        return

    # --- Load progress ---
    if args.fresh:
        progress = {'completed': {}, 'results': []}
    else:
        progress = load_progress()

    completed_keys = set(progress['completed'].keys())
    skipped = 0

    OUTPUT_BASE.mkdir(parents=True, exist_ok=True)

    print(f'\nStarting benchmark ({len(completed_keys)} already completed)...\n')

    for i, (v, encoder, cq, scale) in enumerate(plan, 1):
        key = encode_key(v['path'], encoder, cq, scale)
        if key in completed_keys:
            skipped += 1
            continue

        name = Path(v['path']).name
        scale_label = '1080p' if scale else 'native'
        done = len(completed_keys) + (i - skipped)
        remaining = total_encodes - done - skipped + 1
        print(f'[{i}/{total_encodes}] {name[:45]}  {v["codec"]} -> {encoder} cq{cq} {scale_label}  '
              f'(~{remaining} left)')

        out_dir = make_output_dir(v)
        result = run_encode(v, encoder, cq, scale, out_dir)

        if result:
            progress['results'].append(result)
            progress['completed'][key] = True
            completed_keys.add(key)
            ratio_str = f'{result["compression_ratio"]:.3f}'
            print(f'    -> {human_size(result["output_size"])}  ratio={ratio_str}  time={result["encode_time_s"]}s')
        else:
            # Mark as completed (failed) so we don't retry
            progress['completed'][key] = False

        # Save after every encode
        save_progress(progress)

    # --- Write CSV and summary ---
    write_csv(progress['results'])
    print_summary(progress['results'])

    total_done = sum(1 for v in progress['completed'].values() if v)
    total_failed = sum(1 for v in progress['completed'].values() if not v)
    print(f'Completed: {total_done} successful, {total_failed} failed, {skipped} resumed from cache')


if __name__ == '__main__':
    main()
