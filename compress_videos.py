#!/usr/bin/env python3
"""
Video Compression Script

Recursively finds videos in a folder, converts them to HEVC CQ26 using NVENC,
and moves originals to a backup location.
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

# Video extensions to process
VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.mpeg', '.mpg'}

# Backup location for original files
BACKUP_BASE = Path('/home/brian/share/orig_video_to_delete')

# Metadata tag to mark files we've encoded
ENCODER_TAG = 'compressed_hevc_cq26'


def find_videos(folder: Path) -> list[Path]:
    """Recursively find video files by extension."""
    videos = []
    for path in folder.rglob('*'):
        if path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS:
            videos.append(path)
    return sorted(videos)


def get_video_codec(file: Path) -> str | None:
    """Use ffprobe to detect the video codec."""
    cmd = [
        'ffprobe', '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=codec_name',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        str(file)
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            return result.stdout.strip().lower()
    except subprocess.TimeoutExpired:
        print(f"  Warning: ffprobe timed out for {file}")
    except Exception as e:
        print(f"  Warning: Could not detect codec for {file}: {e}")
    return None


def has_encoder_tag(file: Path) -> bool:
    """Check if file has our encoder metadata tag."""
    cmd = [
        'ffprobe', '-v', 'error',
        '-show_entries', 'format_tags=comment',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        str(file)
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            return ENCODER_TAG in result.stdout.strip()
    except Exception:
        pass
    return False


def compress_video(input_file: Path, output_file: Path) -> bool:
    """Run ffmpeg HEVC encode. Returns True on success."""
    cmd = [
        'ffmpeg', '-y', '-i', str(input_file),
        '-c:v', 'hevc_nvenc', '-cq', '26', '-preset', 'p5',
        '-c:a', 'aac', '-b:a', '128k',
        '-metadata', f'comment={ENCODER_TAG}',
        str(output_file)
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            return True
        else:
            print(f"  ffmpeg error: {result.stderr[-500:] if len(result.stderr) > 500 else result.stderr}")
            return False
    except Exception as e:
        print(f"  ffmpeg exception: {e}")
        return False


def get_unique_path(path: Path) -> Path:
    """Get a unique path by appending a number if file exists."""
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    counter = 1

    while True:
        new_path = parent / f"{stem}_{counter}{suffix}"
        if not new_path.exists():
            return new_path
        counter += 1


def move_to_backup(file: Path) -> bool:
    """Move original file to backup location, preserving full path structure."""
    try:
        # Use absolute path (without leading /) to preserve full directory structure
        # e.g., /home/brian/videos/foo.mp4 -> backup/home/brian/videos/foo.mp4
        absolute_path = file.resolve()
        relative_path = str(absolute_path).lstrip('/')
        backup_path = BACKUP_BASE / relative_path

        # Create backup directory structure
        backup_path.parent.mkdir(parents=True, exist_ok=True)

        # Handle collision in backup folder
        backup_path = get_unique_path(backup_path)

        shutil.move(str(file), str(backup_path))
        return True
    except Exception as e:
        print(f"  Failed to move to backup: {e}")
        return False


def process_video(video: Path, base_folder: Path, current: int = 0, total: int = 0) -> str:
    """
    Process a single video file.
    Returns: 'skipped', 'success', or 'failed'
    """
    progress = f"[{current}/{total}]" if total > 0 else ""
    print(f"\n{progress} Processing: {video}")

    # Check if already encoded by us - only skip if we find our tag
    if has_encoder_tag(video):
        print(f"  Skipping: already encoded with {ENCODER_TAG}")
        return 'skipped'

    # Will encode regardless of current codec
    codec = get_video_codec(video)
    print(f"  Current codec: {codec or 'unknown'} -> re-encoding to HEVC CQ26")

    # Determine output path (same location, .mp4 extension)
    output_file = video.with_suffix('.mp4')

    # Handle case where output would overwrite a different file
    if output_file != video and output_file.exists():
        output_file = get_unique_path(output_file)

    # Use temp file during encoding (keep .mp4 extension so ffmpeg recognizes format)
    temp_file = video.parent / f".{video.stem}.tmp.mp4"

    print(f"  Encoding to: {output_file.name}")

    # Compress
    if not compress_video(video, temp_file):
        # Clean up temp file on failure
        if temp_file.exists():
            temp_file.unlink()
        print("  FAILED: Encoding error")
        return 'failed'

    # Move original to backup
    if not move_to_backup(video):
        # Clean up temp file if backup fails
        if temp_file.exists():
            temp_file.unlink()
        print("  FAILED: Could not backup original")
        return 'failed'

    # Rename temp to final
    try:
        temp_file.rename(output_file)
    except Exception as e:
        print(f"  FAILED: Could not rename temp file: {e}")
        return 'failed'

    print("  SUCCESS")
    return 'success'


def main():
    parser = argparse.ArgumentParser(
        description='Recursively compress videos to HEVC and backup originals.'
    )
    parser.add_argument(
        'folder',
        type=str,
        help='Folder to search for videos'
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Show what would be done without actually processing'
    )

    args = parser.parse_args()

    folder = Path(args.folder).resolve()

    if not folder.exists():
        print(f"Error: Folder does not exist: {folder}")
        sys.exit(1)

    if not folder.is_dir():
        print(f"Error: Not a directory: {folder}")
        sys.exit(1)

    print(f"Searching for videos in: {folder}")
    print(f"Backup location: {BACKUP_BASE}")

    videos = find_videos(folder)
    print(f"Found {len(videos)} video file(s)")

    if not videos:
        print("No videos to process.")
        return

    if args.dry_run:
        print("\n=== DRY RUN ===")
        total = len(videos)
        for i, video in enumerate(videos, start=1):
            if has_encoder_tag(video):
                status = "SKIP (already cq26)"
            else:
                codec = get_video_codec(video)
                status = f"ENCODE ({codec or 'unknown'})"
            print(f"  [{i}/{total}] {status}: {video}")
        return

    # Process videos
    stats = {'success': 0, 'skipped': 0, 'failed': 0}
    total = len(videos)

    for i, video in enumerate(videos, start=1):
        result = process_video(video, folder, current=i, total=total)
        stats[result] += 1

        # Show running totals
        print(f"  Progress: {stats['success']} encoded, {stats['skipped']} skipped, {stats['failed']} failed")

    # Print summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"  Processed (success): {stats['success']}")
    print(f"  Skipped (already HEVC): {stats['skipped']}")
    print(f"  Failed: {stats['failed']}")
    print(f"  Total: {len(videos)}")


if __name__ == '__main__':
    main()
