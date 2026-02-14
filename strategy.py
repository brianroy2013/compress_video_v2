"""
Compression strategy decisions.

Determines whether a video should be encoded, skipped, downscaled,
or remuxed based on its codec, bitrate, resolution, and container.
"""

from pathlib import Path


def is_mp4(file_path):
    """Check if file is an MP4 container."""
    return Path(file_path).suffix.lower() == '.mp4'


def is_4k(width, height):
    """Detect 4K content.

    Uses min(width, height) > 1080 to correctly handle:
    - Landscape 4K: 3840x2160 -> min=2160 > 1080 -> True
    - Portrait 4K: 2160x3840 -> min=2160 > 1080 -> True
    - Portrait 1080p: 1080x1920 -> min=1080 -> False
    - Ultrawide: 2560x1080 -> min=1080 -> False
    """
    if not width or not height:
        return False
    return min(width, height) > 1080


def is_portrait(width, height):
    """Check if video is portrait orientation (taller than wide)."""
    if not width or not height:
        return False
    return height > width


def get_scale_filter(width, height):
    """Return the FFmpeg scale filter for downscaling to 1080p.

    Portrait video: scale=1080:-2  (width=1080, height auto)
    Landscape video: scale=-2:1080 (width auto, height=1080)
    """
    if is_portrait(width, height):
        return 'scale=1080:-2'
    return 'scale=-2:1080'


def decide_action(codec, bitrate, width, height, has_v2_tag=False, ext='.mp4'):
    """Decide what to do with a video.

    Returns a dict with keys:
        action: 'encode', 'skip_tiny', 'skip_tagged'
        target_cq: int or None
        downscale_1080p: bool
        reason: human-readable explanation
        size_gate: bool — whether output must pass the size gate

    Strategy (v4 — H.264 target):
        Non-MP4 4K (any codec)    -> encode H.264 + downscale, no size gate
        Non-MP4 non-4K            -> encode H.264, no size gate
        MP4 any 4K                -> encode H.264 + downscale, size gate
        MP4 HEVC non-4K           -> encode H.264, no size gate (compatibility)
        MP4 AV1 non-4K            -> encode H.264, no size gate (compatibility)
        MP4 H.264 < 0.5 Mbps     -> skip (tiny)
        MP4 H.264 >= 0.5 Mbps    -> encode H.264, size gate
        MP4 VP9/other             -> encode H.264, size gate
    """
    if has_v2_tag:
        return {
            'action': 'skip_tagged',
            'target_cq': None,
            'downscale_1080p': False,
            'size_gate': False,
            'reason': 'Already compressed with v2',
        }

    codec_lower = (codec or '').lower()
    bitrate_mbps = (bitrate or 0) / 1_000_000
    mp4 = (ext or '').lower() == '.mp4'

    # ── Non-MP4 files: always encode to H.264 MP4 ──
    if not mp4:
        if is_4k(width, height):
            return {
                'action': 'encode',
                'target_cq': 30,
                'downscale_1080p': True,
                'size_gate': False,
                'reason': f'Non-MP4 4K {width}x{height} -> encode H.264 + downscale',
            }
        return {
            'action': 'encode',
            'target_cq': 30,
            'downscale_1080p': False,
            'size_gate': False,
            'reason': f'Non-MP4 {codec_lower} -> encode to H.264 MP4',
        }

    # ── MP4 files ──

    # 4K MP4: always encode + downscale
    if is_4k(width, height):
        return {
            'action': 'encode',
            'target_cq': 30,
            'downscale_1080p': True,
            'size_gate': True,
            'reason': f'4K {width}x{height} -> H.264 + downscale to 1080p',
        }

    if codec_lower == 'h264':
        if bitrate_mbps >= 2:
            return {
                'action': 'encode',
                'target_cq': 30,
                'downscale_1080p': False,
                'size_gate': True,
                'reason': f'H.264 at {bitrate_mbps:.1f} Mbps (high)',
            }
        elif bitrate_mbps >= 0.5:
            return {
                'action': 'encode',
                'target_cq': 30,
                'downscale_1080p': False,
                'size_gate': True,
                'reason': f'H.264 at {bitrate_mbps:.1f} Mbps (mid)',
            }
        else:
            return {
                'action': 'skip_tiny',
                'target_cq': None,
                'downscale_1080p': False,
                'size_gate': False,
                'reason': f'H.264 at {bitrate_mbps:.2f} Mbps (too low)',
            }

    if codec_lower == 'hevc':
        return {
            'action': 'encode',
            'target_cq': 30,
            'downscale_1080p': False,
            'size_gate': False,
            'reason': 'HEVC MP4 -> encode to H.264 (compatibility)',
        }

    if codec_lower == 'av1':
        return {
            'action': 'encode',
            'target_cq': 30,
            'downscale_1080p': False,
            'size_gate': False,
            'reason': 'AV1 MP4 -> encode to H.264 (compatibility)',
        }

    if codec_lower == 'vp9':
        return {
            'action': 'encode',
            'target_cq': 30,
            'downscale_1080p': False,
            'size_gate': True,
            'reason': 'VP9 -> H.264',
        }

    # Fallback for other codecs (mpeg4, msmpeg4v3, etc.)
    return {
        'action': 'encode',
        'target_cq': 30,
        'downscale_1080p': False,
        'size_gate': True,
        'reason': f'{codec_lower} -> H.264',
    }
