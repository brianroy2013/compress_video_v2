"""
Multi-machine claim coordination via NAS.

Uses atomic file creation (O_CREAT | O_EXCL) on the NAS mount
to ensure only one machine processes a given video at a time.
Works reliably on CIFS even though SQLite locking doesn't.
"""

import hashlib
import json
import os
import socket
import time
from datetime import datetime, timedelta
from pathlib import Path

CLAIM_DIR = Path('/home/brian/share/.compress_claims')
HOSTNAME = socket.gethostname()
STALE_HOURS = 24


def _claim_path(video_path):
    """Get the claim file path for a video."""
    h = hashlib.sha256(str(video_path).encode()).hexdigest()
    return CLAIM_DIR / f'{h}.claim'


def ensure_claim_dir():
    """Create the claim directory if it doesn't exist."""
    CLAIM_DIR.mkdir(parents=True, exist_ok=True)


def claim_file(video_path):
    """Try to atomically claim a video for processing.

    Returns True if we got the claim, False if already claimed.
    Uses O_CREAT | O_EXCL for race-free creation even on CIFS.
    """
    ensure_claim_dir()
    cp = _claim_path(video_path)
    claim_data = json.dumps({
        'hostname': HOSTNAME,
        'video_path': str(video_path),
        'claimed_at': datetime.now().isoformat(),
    })

    try:
        fd = os.open(str(cp), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, claim_data.encode())
        os.close(fd)
        return True
    except FileExistsError:
        return False
    except OSError as e:
        # CIFS may raise errno 17 (EEXIST) as a generic OSError
        if e.errno == 17:
            return False
        raise


def is_claimed(video_path):
    """Check if a video is claimed by any machine."""
    return _claim_path(video_path).exists()


def read_claim(video_path):
    """Read claim data for a video. Returns dict or None."""
    cp = _claim_path(video_path)
    if not cp.exists():
        return None
    try:
        return json.loads(cp.read_text())
    except Exception:
        return None


def release_claim(video_path):
    """Release a claim (delete the claim file)."""
    cp = _claim_path(video_path)
    try:
        cp.unlink()
    except FileNotFoundError:
        pass


def recover_stale(max_hours=STALE_HOURS, dry_run=False):
    """Find and release claims older than max_hours.

    Returns list of (claim_path, claim_data) that were released.
    """
    ensure_claim_dir()
    cutoff = datetime.now() - timedelta(hours=max_hours)
    recovered = []

    for cp in CLAIM_DIR.glob('*.claim'):
        try:
            data = json.loads(cp.read_text())
            claimed_at = datetime.fromisoformat(data['claimed_at'])
            if claimed_at < cutoff:
                if not dry_run:
                    cp.unlink()
                recovered.append((str(cp), data))
        except Exception:
            # Corrupted claim file -- release it
            if not dry_run:
                try:
                    cp.unlink()
                except Exception:
                    pass
            recovered.append((str(cp), {'error': 'corrupted'}))

    return recovered


def list_claims():
    """List all active claims. Returns list of dicts."""
    ensure_claim_dir()
    claims = []
    for cp in CLAIM_DIR.glob('*.claim'):
        try:
            data = json.loads(cp.read_text())
            data['claim_file'] = str(cp)
            claims.append(data)
        except Exception:
            claims.append({
                'claim_file': str(cp),
                'error': 'corrupted',
            })
    return claims
