#!/usr/bin/env python3
"""
Video Compression Script

Recursively finds videos in a folder, converts them to HEVC CQ26 using NVENC,
and moves originals to a backup location.
"""

import argparse
import json
import logging
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Video extensions to process
VIDEO_EXTENSIONS = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.mpeg', '.mpg'}

# Backup location for original files
BACKUP_BASE = Path('/home/brian/share/orig_video_to_delete')

# Metadata tag to mark files we've encoded
ENCODER_TAG = 'compressed_hevc_cq26'

# Error log file location
ERROR_LOG_FILE = Path('/home/brian/userfiles/claude/compres_video/compression_errors.log')

# Configure logging
error_logger = logging.getLogger('compression_errors')
error_logger.setLevel(logging.ERROR)


def setup_error_logging():
    """Set up file handler for error logging."""
    ERROR_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(ERROR_LOG_FILE, mode='a')
    handler.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)
    error_logger.addHandler(handler)


def get_file_info(file: Path) -> dict:
    """Get file information for error logging."""
    info = {
        'path': str(file),
        'exists': file.exists(),
    }
    if file.exists():
        stat = file.stat()
        info['size_bytes'] = stat.st_size
        info['size_mb'] = round(stat.st_size / (1024 * 1024), 2)
        info['modified'] = datetime.fromtimestamp(stat.st_mtime).isoformat()
    return info


def log_error(error_type: str, file: Path, details: dict):
    """Log an error with full context for debugging."""
    error_entry = {
        'timestamp': datetime.now().isoformat(),
        'error_type': error_type,
        'file_info': get_file_info(file),
        'details': details,
    }

    # Write as formatted JSON for readability
    error_logger.error("=" * 80)
    error_logger.error(json.dumps(error_entry, indent=2))
    error_logger.error("")


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
        else:
            log_error('ffprobe_codec_detection', file, {
                'command': ' '.join(cmd),
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
            })
    except subprocess.TimeoutExpired:
        print(f"  Warning: ffprobe timed out for {file}")
        log_error('ffprobe_timeout', file, {
            'command': ' '.join(cmd),
            'timeout_seconds': 30,
        })
    except Exception as e:
        print(f"  Warning: Could not detect codec for {file}: {e}")
        log_error('ffprobe_exception', file, {
            'command': ' '.join(cmd),
            'exception_type': type(e).__name__,
            'exception_message': str(e),
        })
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
            log_error('ffmpeg_encoding_failed', input_file, {
                'command': ' '.join(cmd),
                'output_file': str(output_file),
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
            })
            return False
    except Exception as e:
        print(f"  ffmpeg exception: {e}")
        log_error('ffmpeg_exception', input_file, {
            'command': ' '.join(cmd),
            'output_file': str(output_file),
            'exception_type': type(e).__name__,
            'exception_message': str(e),
        })
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
    backup_path = None
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
        log_error('backup_move_failed', file, {
            'backup_path': str(backup_path) if backup_path else None,
            'backup_base': str(BACKUP_BASE),
            'exception_type': type(e).__name__,
            'exception_message': str(e),
        })
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
        log_error('temp_rename_failed', video, {
            'temp_file': str(temp_file),
            'output_file': str(output_file),
            'temp_exists': temp_file.exists(),
            'output_exists': output_file.exists(),
            'exception_type': type(e).__name__,
            'exception_message': str(e),
        })
        return 'failed'

    print("  SUCCESS")
    return 'success'


def main():
    setup_error_logging()

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

    if stats['failed'] > 0:
        print(f"\n  Error details logged to: {ERROR_LOG_FILE}")


if __name__ == '__main__':
    main()
