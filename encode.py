"""
FFmpeg encoding pipeline.

Handles GPU-accelerated HEVC encoding, size gating, and backup management.
"""

import json
import shutil
import subprocess
from pathlib import Path

BACKUP_BASE = Path('/home/brian/share/orig_video_to_delete')
ENCODER_TAG = 'compressed_hevc_v2'
MIN_SAVINGS_PCT = 5.0

# GPU decoders by source codec
GPU_DECODERS = {
    'h264': 'h264_cuvid',
    'hevc': 'hevc_cuvid',
    'av1': 'av1_cuvid',
}


def ffprobe_info(file_path):
    """Probe a video file. Returns dict with codec, width, height, bitrate, duration, or None."""
    cmd = [
        'ffprobe', '-v', 'error',
        '-select_streams', 'v:0',
        '-show_entries', 'stream=codec_name,width,height,bit_rate',
        '-show_entries', 'format=duration,bit_rate,size',
        '-show_entries', 'format_tags=comment',
        '-print_format', 'json',
        str(file_path)
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

        comment = fmt.get('tags', {}).get('comment', '')

        return {
            'codec': stream.get('codec_name', 'unknown'),
            'width': width,
            'height': height,
            'bitrate': bitrate,
            'duration': duration,
            'comment': comment,
            'size': int(fmt.get('size', 0) or 0),
        }
    except Exception:
        return None


def has_v2_tag(file_path):
    """Check if file already has our v2 encoder tag."""
    info = ffprobe_info(file_path)
    if info and info.get('comment'):
        return ENCODER_TAG in info['comment']
    return False


def build_cmd(input_path, output_path, source_codec, target_cq, scale_filter=None):
    """Build the ffmpeg command for encoding.

    Args:
        input_path: Source video file
        output_path: Temp output file
        source_codec: Source codec name (for GPU decoder selection)
        target_cq: CQ value for hevc_nvenc
        scale_filter: Optional scale filter string (e.g. 'scale=-2:1080')

    Returns:
        List of command arguments
    """
    cmd = ['ffmpeg', '-y']

    # GPU decoder if available
    gpu_dec = GPU_DECODERS.get(source_codec)
    if gpu_dec:
        cmd.extend(['-c:v', gpu_dec])

    cmd.extend(['-i', str(input_path)])

    # Video encoder
    cmd.extend(['-c:v', 'hevc_nvenc', '-cq', str(target_cq), '-preset', 'p5'])

    # Scale filter for 4K downscale
    if scale_filter:
        cmd.extend(['-vf', scale_filter])

    # Audio
    cmd.extend(['-c:a', 'aac', '-b:a', '128k'])

    # Metadata tag
    cmd.extend(['-metadata', f'comment={ENCODER_TAG}'])

    cmd.append(str(output_path))
    return cmd


def encode_video(input_path, source_codec, target_cq, scale_filter=None):
    """Encode a video file.

    Returns dict:
        success: bool
        output_path: Path to output (only if success and passed size gate)
        output_size: int
        savings_pct: float
        passed_size_gate: bool
        error: str (only on failure)
    """
    input_path = Path(input_path)
    input_size = input_path.stat().st_size

    # Temp file in same directory
    temp_file = input_path.parent / f'.{input_path.stem}.tmp.mp4'

    cmd = build_cmd(input_path, temp_file, source_codec, target_cq, scale_filter)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
        if result.returncode != 0:
            _cleanup(temp_file)
            stderr_tail = result.stderr[-1000:] if len(result.stderr) > 1000 else result.stderr
            return {
                'success': False,
                'error': f'ffmpeg exit {result.returncode}: {stderr_tail}',
            }
    except subprocess.TimeoutExpired:
        _cleanup(temp_file)
        return {'success': False, 'error': 'ffmpeg timed out (2h)'}
    except Exception as e:
        _cleanup(temp_file)
        return {'success': False, 'error': str(e)}

    # Size gate
    if not temp_file.exists():
        return {'success': False, 'error': 'Output file not created'}

    output_size = temp_file.stat().st_size
    savings_pct = (1 - output_size / input_size) * 100 if input_size > 0 else 0

    if savings_pct < MIN_SAVINGS_PCT:
        _cleanup(temp_file)
        return {
            'success': True,
            'passed_size_gate': False,
            'output_size': output_size,
            'savings_pct': savings_pct,
            'error': None,
        }

    # Output is in same directory as input, same name but .mp4
    final_path = input_path.with_suffix('.mp4')

    # If the input file IS already .mp4, we'll replace it after backup
    # If there's a different file at that path, make it unique
    if final_path != input_path and final_path.exists():
        final_path = _unique_path(final_path)

    return {
        'success': True,
        'passed_size_gate': True,
        'temp_path': str(temp_file),
        'final_path': str(final_path),
        'output_size': output_size,
        'savings_pct': savings_pct,
        'error': None,
    }


def move_to_backup(file_path):
    """Move original file to backup location, preserving path structure.

    Uses shutil.move() for cross-filesystem support.
    Returns backup path on success, None on failure.
    """
    file_path = Path(file_path)
    absolute_path = file_path.resolve()
    relative_path = str(absolute_path).lstrip('/')
    backup_path = BACKUP_BASE / relative_path

    backup_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path = _unique_path(backup_path)

    try:
        shutil.move(str(file_path), str(backup_path))
        return str(backup_path)
    except Exception as e:
        return None


def finalize(temp_path, final_path):
    """Rename temp file to final output path.

    Uses shutil.move() for cross-filesystem support.
    Returns True on success.
    """
    try:
        shutil.move(str(temp_path), str(final_path))
        return True
    except Exception:
        return False


def get_backup_path(original_path):
    """Derive the expected backup path for a given original file path."""
    absolute_path = Path(original_path).resolve()
    relative_path = str(absolute_path).lstrip('/')
    return BACKUP_BASE / relative_path


def _unique_path(path):
    """Get a unique path by appending _N if file exists."""
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    counter = 1
    while True:
        new_path = parent / f'{stem}_{counter}{suffix}'
        if not new_path.exists():
            return new_path
        counter += 1


def _cleanup(path):
    """Remove a file if it exists."""
    try:
        Path(path).unlink()
    except Exception:
        pass
