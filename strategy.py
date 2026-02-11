"""
Compression strategy decisions.

Determines whether a video should be encoded, skipped, or downscaled
based on its codec, bitrate, and resolution.
"""


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


def decide_action(codec, bitrate, width, height, has_v2_tag=False):
    """Decide what to do with a video.

    Returns a dict with keys:
        action: 'encode', 'skip_hevc', 'skip_av1', 'skip_tiny', 'skip_tagged'
        target_cq: int or None
        downscale_1080p: bool
        reason: human-readable explanation

    Strategy table:
        Any codec, 4K          -> encode + downscale, CQ 26
        H.264, >= 2 Mbps       -> encode, CQ 28
        H.264, 0.5 - 2 Mbps   -> encode + size gate, CQ 30
        H.264, < 0.5 Mbps      -> skip_tiny
        HEVC, non-4K           -> skip_hevc
        AV1, non-4K            -> skip_av1
        VP9, any               -> encode, CQ 30
        Other                  -> encode, CQ 28
    """
    if has_v2_tag:
        return {
            'action': 'skip_tagged',
            'target_cq': None,
            'downscale_1080p': False,
            'reason': 'Already compressed with v2',
        }

    codec_lower = (codec or '').lower()
    bitrate_mbps = (bitrate or 0) / 1_000_000

    # 4K content always gets downscaled regardless of codec
    if is_4k(width, height):
        return {
            'action': 'encode',
            'target_cq': 26,
            'downscale_1080p': True,
            'reason': f'4K {width}x{height} -> downscale to 1080p',
        }

    if codec_lower == 'h264':
        if bitrate_mbps >= 2:
            return {
                'action': 'encode',
                'target_cq': 28,
                'downscale_1080p': False,
                'reason': f'H.264 at {bitrate_mbps:.1f} Mbps (high)',
            }
        elif bitrate_mbps >= 0.5:
            return {
                'action': 'encode',
                'target_cq': 30,
                'downscale_1080p': False,
                'reason': f'H.264 at {bitrate_mbps:.1f} Mbps (mid, size gate)',
            }
        else:
            return {
                'action': 'skip_tiny',
                'target_cq': None,
                'downscale_1080p': False,
                'reason': f'H.264 at {bitrate_mbps:.2f} Mbps (too low)',
            }

    if codec_lower == 'hevc':
        return {
            'action': 'skip_hevc',
            'target_cq': None,
            'downscale_1080p': False,
            'reason': 'Already HEVC, non-4K',
        }

    if codec_lower == 'av1':
        return {
            'action': 'skip_av1',
            'target_cq': None,
            'downscale_1080p': False,
            'reason': 'Already AV1, non-4K',
        }

    if codec_lower == 'vp9':
        return {
            'action': 'encode',
            'target_cq': 30,
            'downscale_1080p': False,
            'reason': 'VP9 -> HEVC',
        }

    # Fallback for other codecs (mpeg4, msmpeg4v3, etc.)
    return {
        'action': 'encode',
        'target_cq': 28,
        'downscale_1080p': False,
        'reason': f'{codec_lower} -> HEVC',
    }
