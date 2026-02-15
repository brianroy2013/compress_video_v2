# Video Compression v3

GPU-accelerated video compression tool using H.264 encoding via NVIDIA NVENC. Designed for batch processing large video libraries across multiple machines on a NAS.

## How It Works

Instead of a database, the system uses **metadata tags** and **filename conventions** to track state:

| Indicator | Meaning |
|-----------|---------|
| `compressed_h264_v4` metadata tag | Successfully compressed — output keeps its base name (e.g. `video.avi` → `video.mp4`) |
| `_skip.mp4` filename suffix | Skipped (already efficient, too small, or no savings from re-encoding) |
| *(no tag, no suffix)* | Not yet processed |

Re-running is always safe — files with `_skip` in the name are ignored, and files already tagged `compressed_h264_v4` are skipped. Old HEVC `_compressed` files without the new tag will be re-encoded to H.264.

## Commands

### `run` — Compress videos

Discovers video files, decides the best action for each, and processes them.

```bash
# Process all files in a folder
python3 compress_v3.py run /home/brian/share/videos

# Process a specific subfolder
python3 compress_v3.py run /home/brian/share/videos/poker

# Multiple folders
python3 compress_v3.py run /home/brian/share/folder1 /home/brian/share/folder2

# Limit to N files per run (processes largest first)
python3 compress_v3.py run /home/brian/share/videos --batch 50

# Preview what would happen without making changes
python3 compress_v3.py run /home/brian/share/videos --dry-run

# Release stale claims (>24h old) from crashed machines before starting
python3 compress_v3.py run /home/brian/share/videos --recover-stale
```

**What `run` does for each file:**

| File Type | Action |
|-----------|--------|
| Non-MP4 4K (any codec) | Encode to H.264 + downscale to 1080p |
| Non-MP4 non-4K (any codec) | Encode to H.264 MP4 |
| MP4 4K (any codec) | Encode to H.264 + downscale to 1080p |
| MP4 HEVC (non-4K) | Encode to H.264 (compatibility) |
| MP4 AV1 (non-4K) | Encode to H.264 (compatibility) |
| MP4 H.264 < 0.5 Mbps | Skip (rename to `_skip.mp4`) |
| MP4 H.264 >= 0.5 Mbps | Encode to H.264 (must save >= 5% or becomes `_skip`) |
| MP4 VP9/other | Encode to H.264 (must save >= 5% or becomes `_skip`) |

Files are processed largest-first. Originals are moved to `/home/brian/share/orig_video_to_delete/` as a backup.

### `status` — Check progress

Walks the filesystem and counts files by state.

```bash
python3 compress_v3.py status /home/brian/share/videos

# Check a specific subfolder
python3 compress_v3.py status /home/brian/share/videos/poker
```

Output shows compressed, skipped, in-progress (claimed), and remaining file counts with total sizes.

### `migrate` — Rename v2-compressed files

One-time command to rename files that were compressed by the old v2 system. Detects them by reading the `compressed_hevc_v2` metadata tag embedded in each file.

```bash
# Preview what would be renamed
python3 compress_v3.py migrate /home/brian/share --dry-run

# Run it
python3 compress_v3.py migrate /home/brian/share

# Target a specific subfolder to go faster
python3 compress_v3.py migrate /home/brian/share/videos/poker
```

Safe to stop (Ctrl+C) and restart — already-renamed files (with `_compressed` in the name) are skipped. To resume faster, pass the specific subfolder(s) you haven't finished yet rather than the top-level share.

## Multi-Machine Support

Multiple machines can run `compress_v3.py run` on the same NAS folders simultaneously. Coordination uses atomic claim files on the NAS:

- Before processing a file, a machine creates a claim file in `/home/brian/share/.compress_claims/`
- Other machines see the claim and skip that file
- Claims are released when processing completes (success or failure)
- Use `--recover-stale` to release claims >24 hours old (from crashed/killed processes)

## Stopping and Restarting

All commands are safe to interrupt with Ctrl+C and restart:

- **`run`**: The current file being encoded may leave a temp file (`.{name}.tmp.mp4`) which is cleaned up on next run. The claim is released. No files are lost.
- **`migrate`**: Each rename is independent. Re-running skips already-renamed files.
- **`status`**: Read-only, no side effects.

## Supported Video Formats

`.mp4`, `.mkv`, `.avi`, `.mov`, `.wmv`, `.flv`, `.webm`, `.m4v`, `.mpeg`, `.mpg`

## File Layout

```
compress_v3.py   — Main CLI (run, status, migrate)
encode.py        — FFmpeg encoding, remuxing, backup, ffprobe
strategy.py      — Decision logic (encode vs skip vs remux)
claim.py         — Multi-machine NAS claim coordination
```
