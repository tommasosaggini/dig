#!/usr/bin/env python3
"""
DIG → Instagram: render a queued item into publishable media.

For one item it produces, under media/ig/<id>/:
    clip.mp3    — the chosen 30s window cut from source.mp3
    card_feed.png / card_story.png — the visual (artwork + gradient + text)
    feed.mp4    — 1080x1080 still-over-audio (Reel-eligible)
    story.mp4   — 1080x1920 still-over-audio (Stories)

This is the DRY-RUN heart of the pipeline: it makes real posts you can preview
in the dashboard BEFORE any Instagram wiring exists.

Requires: Pillow (pip) + ffmpeg (system).

Usage:
    python3 pipeline/ig_render.py            # render all ready/scheduled, unrendered
    python3 pipeline/ig_render.py --id 12    # render one item
"""
import datetime
import os
import subprocess
import sys
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

ENV_PATH = os.path.join(ROOT, ".env")
if os.path.exists(ENV_PATH):
    with open(ENV_PATH) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

from lib import ig_queue

FEED = (1080, 1080)
STORY = (1080, 1920)
_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"

# Candidate fonts (first that exists wins). macOS + Linux container both covered.
_FONT_CANDIDATES = [
    "/System/Library/Fonts/Helvetica.ttc",
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
]


def _font(size):
    from PIL import ImageFont
    for path in _FONT_CANDIDATES:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                continue
    return ImageFont.load_default()


def _ffmpeg_ok():
    from shutil import which
    return which("ffmpeg") is not None


def cut_clip(source_path, start_ms, dur_ms, dest):
    """Trim [start, start+dur] from source audio into a fresh mp3."""
    start_s = max(0, start_ms) / 1000.0
    dur_s = max(1, dur_ms) / 1000.0
    cmd = [
        "ffmpeg", "-y", "-ss", f"{start_s:.3f}", "-t", f"{dur_s:.3f}",
        "-i", source_path, "-acodec", "libmp3lame", "-b:a", "192k", dest,
    ]
    subprocess.run(cmd, check=True, capture_output=True)
    return dest


def _download_art(url, dest):
    if not url:
        return None
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=30) as resp, open(dest, "wb") as f:
            f.write(resp.read())
        return dest
    except Exception:
        return None


def _fit_text(draw, text, font_path_size, max_width):
    """Greedy word-wrap to fit max_width at the given font size."""
    from PIL import ImageFont
    font = _font(font_path_size)
    words = text.split()
    lines, cur = [], ""
    for w in words:
        trial = (cur + " " + w).strip()
        if draw.textlength(trial, font=font) <= max_width or not cur:
            cur = trial
        else:
            lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines, font


def render_card(track_name, artist, art_path, size, dest,
                labels=None, track_id=None):
    """Compose the post card: the track's generated abstract cover, with the
    title, artist and DIG mark set over it.

    The picture comes from lib/ig_artwork — the record's palette, distorted by
    the labels the discovery engine already assigned. The sleeve itself is
    never shown, only the colours taken from it.
    """
    from PIL import Image, ImageDraw
    from lib import ig_artwork
    W, H = size

    art = None
    if art_path and os.path.exists(art_path):
        try:
            art = Image.open(art_path).convert("RGB")
        except Exception:
            art = None

    canvas, _used = ig_artwork.generate(art, size, labels or {},
                                        track_id or track_name)
    canvas = canvas.convert("RGB")

    # Scrim under the type. The generated field is unpredictable by design, so
    # the text needs its own guaranteed contrast rather than trusting the art.
    scrim = Image.new("L", size, 0)
    sdraw = ImageDraw.Draw(scrim)
    top = int(H * 0.52)
    for y in range(top, H):
        t = (y - top) / max(1, H - top)
        sdraw.line([(0, y), (W, y)], fill=int(215 * (t ** 1.4)))
    canvas = Image.composite(Image.new("RGB", size, (6, 6, 8)), canvas, scrim)

    draw = ImageDraw.Draw(canvas)
    text_top = int(H * (0.66 if size == FEED else 0.70))

    # Title (wrapped) + artist.
    margin = int(W * 0.08)
    maxw = W - 2 * margin
    title_lines, tfont = _fit_text(draw, track_name, int(W * 0.072), maxw)
    y = text_top
    for ln in title_lines[:3]:
        draw.text((margin, y), ln, font=tfont, fill=(245, 245, 245))
        y += int(tfont.size * 1.15)
    y += int(H * 0.012)
    afont = _font(int(W * 0.046))
    artist_lines, afont = _fit_text(draw, artist, int(W * 0.046), maxw)
    for ln in artist_lines[:2]:
        draw.text((margin, y), ln, font=afont, fill=(180, 180, 185))
        y += int(afont.size * 1.2)

    # DIG mark, bottom.
    mark = _font(int(W * 0.034))
    label = "dig ·  diiiiiiiig.xyz"
    mw = draw.textlength(label, font=mark)
    draw.text(((W - mw) / 2, H - int(H * 0.07)), label, font=mark, fill=(120, 120, 125))

    canvas.save(dest, "PNG")
    return dest


def _cover(img, size):
    """Resize+crop `img` to fill `size` (cover), centred."""
    from PIL import Image
    W, H = size
    iw, ih = img.size
    scale = max(W / iw, H / ih)
    nw, nh = int(iw * scale), int(ih * scale)
    img2 = img.resize((nw, nh), Image.LANCZOS)
    left, top = (nw - W) // 2, (nh - H) // 2
    return img2.crop((left, top, left + W, top + H))


def mux_video(card_png, clip_mp3, size, dest):
    """Still image + audio → H.264/AAC mp4 sized for the target format."""
    W, H = size
    cmd = [
        "ffmpeg", "-y", "-loop", "1", "-i", card_png, "-i", clip_mp3,
        "-c:v", "libx264", "-tune", "stillimage", "-pix_fmt", "yuv420p",
        "-vf", f"scale={W}:{H}", "-r", "30",
        "-c:a", "aac", "-b:a", "192k", "-shortest", dest,
    ]
    subprocess.run(cmd, check=True, capture_output=True)
    return dest


def track_labels(track_id):
    """The discovery labels for a track — they drive the generated cover.

    The queue row doesn't carry them (it predates the artwork work), so read
    them back from `tracks`. A missing row is fine: ig_artwork falls back to a
    seeded transform rather than refusing to draw.
    """
    if not track_id:
        return {}
    from lib.db import fetchone
    row = fetchone(
        "SELECT label_energy, label_mood, label_texture, label_feel "
        "FROM tracks WHERE id = %s", (track_id,))
    if not row:
        return {}
    return {"energy": row["label_energy"], "mood": row["label_mood"],
            "texture": row["label_texture"], "feel": row["label_feel"]}


def render_item(item):
    """Full render for one queue item. Returns dict of produced paths."""
    if not _ffmpeg_ok():
        raise RuntimeError("ffmpeg not found on PATH — required for clip + mux")
    iid = item["id"]
    d = ig_queue.item_dir(iid)
    os.makedirs(d, exist_ok=True)
    source = item.get("audio_path")
    if not source or not os.path.exists(source):
        raise RuntimeError(f"source audio missing for item {iid}")
    start = item.get("clip_start_ms")
    if start is None:
        raise RuntimeError(f"no clip window picked for item {iid}")
    dur = item.get("clip_duration_ms") or 30000

    clip = cut_clip(source, start, dur, os.path.join(d, "clip.mp3"))
    art = _download_art(item.get("artwork_url"), os.path.join(d, "art.jpg"))
    labels = track_labels(item.get("track_id"))
    tid = item.get("track_id") or iid

    produced = {"clip": clip}
    if item.get("post_feed", True):
        cf = render_card(item["track_name"], item["artist"], art, FEED,
                         os.path.join(d, "card_feed.png"), labels, tid)
        produced["feed_card"] = cf
        produced["feed"] = mux_video(cf, clip, FEED, os.path.join(d, "feed.mp4"))
    if item.get("post_story", True):
        cs = render_card(item["track_name"], item["artist"], art, STORY,
                         os.path.join(d, "card_story.png"), labels, tid)
        produced["story_card"] = cs
        produced["story"] = mux_video(cs, clip, STORY, os.path.join(d, "story.mp4"))

    ig_queue.update_item(
        iid, rendered_at=datetime.datetime.now(datetime.timezone.utc))
    return produced


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", type=int, help="render a single item id")
    ap.add_argument("--limit", type=int, default=20)
    args = ap.parse_args()

    if args.id:
        item = ig_queue.get_item(args.id)
        if not item:
            print(f"no item {args.id}")
            return
        items = [item]
    else:
        items = ig_queue.items_needing_render(args.limit)

    if not items:
        print("nothing to render.")
        return
    for it in items:
        try:
            out = render_item(it)
            print(f"  rendered #{it['id']} {it['track_name']} → {', '.join(out)}")
        except Exception as e:
            ig_queue.update_item(it["id"], error=str(e)[:500])
            print(f"  FAILED #{it['id']}: {e!r}")


if __name__ == "__main__":
    main()
