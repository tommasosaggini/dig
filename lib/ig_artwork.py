"""
Label-driven abstract cover art for Dig's Instagram posts.

The picture is a portrait of the track, generated from the same labels the
discovery engine already assigns — no new analysis, no stock decoration:

    energy   → how violently the field is disturbed        (amplitude)
    texture  → WHICH distortions run, up to two, layered   (the character)
    mood     → how the palette is treated                  (the colour)
    feel     → composition and light                       (the staging)

The base is the track's own cover reduced to a 12x12 colour field and smoothly
upscaled: the artwork's colour *composition* survives, the imagery does not.
That keeps every card tied to its record while never republishing a label's
cover art — the takedown risk flagged in docs/IG_PIPELINE_PLAN.md §2.

Randomness is seeded from the track id, so a given track always renders the
same picture, while two tracks sharing every label still diverge.
"""
import colorsys
import hashlib

import numpy as np

BASE_GRID = 12          # cover is reduced to this before being blown back up
_EPS = 1e-6


# ── inputs ────────────────────────────────────────────────────────────────────

def _rng(track_id):
    """Deterministic per-track RNG — same track, same picture, forever."""
    h = hashlib.sha256(str(track_id).encode()).digest()
    return np.random.default_rng(int.from_bytes(h[:8], "big"))


ENERGY_AMPLITUDE = {
    "very low": 0.25, "low": 0.55, "moderate": 1.0,
    "high": 1.7, "very high": 2.5,
}

# Each texture token names one transform. Tracks usually carry two
# ("acoustic wood, airy vocals"), so the transforms compose.
TEXTURE_TRANSFORMS = {
    "deep bass": "wave",
    "shimmering synths": "ripple",
    "punchy drums": "slice",
    "percussive tribal": "shards",
    "airy vocals": "bloom",
    "raw distorted": "tear",
    "acoustic wood": "grain",
    "warm analog": "halation",
    "crisp digital": "quantise",
    "clean electric": "streak",
}

# Mood → (saturation, contrast, hue rotation, lift toward white)
MOOD_COLOUR = {
    "euphoric":    (1.9, 1.15, 0.04, 0.02),
    "joyful":      (1.7, 1.10, 0.02, 0.05),
    "playful":     (1.8, 1.05, 0.08, 0.04),
    "rebellious":  (1.3, 1.45, -0.03, -0.05),
    "aggressive":  (1.1, 1.70, -0.05, -0.10),
    "warm":        (1.3, 1.00, 0.05, 0.04),
    "spiritual":   (0.9, 0.95, 0.07, 0.10),
    "tender":      (0.8, 0.80, 0.03, 0.14),
    "serene":      (0.7, 0.75, 0.01, 0.16),
    "dreamy":      (0.9, 0.80, 0.10, 0.12),
    "nostalgic":   (0.5, 0.90, 0.06, 0.08),
    "bittersweet": (0.6, 0.95, -0.02, 0.02),
    "melancholic": (0.35, 1.00, -0.06, -0.02),
    "mysterious":  (0.30, 1.20, -0.08, -0.08),
}

# Feel → staging. Anything unlisted falls back to a soft vignette.
FEEL_STAGING = {
    "empty cathedral":     "mirror",
    "stargazing":          "mirror",
    "candlelit room":      "warm_vignette",
    "rainy afternoon":     "warm_vignette",
    "rooftop sunset":      "horizon",
    "city night":          "horizon",
    "crowded dancefloor":  "smear",
    "festival main stage": "smear",
    "basement show":       "crush",
    "foggy street":        "haze",
}


def _tokens(value):
    """'acoustic wood, airy vocals' → ['acoustic wood', 'airy vocals']."""
    if not value:
        return []
    return [t.strip().lower() for t in str(value).split(",") if t.strip()]


# ── base field ────────────────────────────────────────────────────────────────

PALETTE_K = 5
_SAT_FLOOR = 0.45       # covers that are grey still have to produce colour
_VAL_FLOOR = 0.30


def extract_palette(art_img, rng, k=PALETTE_K):
    """The cover's dominant colours, pushed to a usable saturation.

    Averaging a photograph gives mud, so we quantise to k colours instead and
    then force each one to carry real chroma — a monochrome sleeve should still
    yield a picture worth looking at, seeded from the track so it stays stable.
    """
    from PIL import Image
    if art_img is None:
        base_h = float(rng.random())
        return [colorsys.hsv_to_rgb((base_h + i / float(k) * 0.45) % 1.0,
                                    0.72, 0.45 + 0.5 * (i / max(1, k - 1)))
                for i in range(k)]

    q = art_img.convert("RGB").resize((96, 96), Image.BOX).quantize(
        colors=k, method=Image.MEDIANCUT)
    pal = q.getpalette()[: k * 3]
    counts = sorted(q.getcolors() or [], reverse=True)
    order = [idx for _, idx in counts] or list(range(k))

    out, base_h = [], float(rng.random())
    for n, idx in enumerate(order[:k]):
        r, g, b = [c / 255.0 for c in pal[idx * 3: idx * 3 + 3]]
        h, s, v = colorsys.rgb_to_hsv(r, g, b)
        if s < _SAT_FLOOR:
            # Near-grey: keep its lightness, borrow a hue from the track seed
            # so the family still reads as one system.
            h = (base_h + n * 0.11) % 1.0
            s = _SAT_FLOOR + 0.30 * float(rng.random())
        else:
            s = min(1.0, s * 1.35)
        v = max(_VAL_FLOOR, min(1.0, v * 1.10))
        out.append(colorsys.hsv_to_rgb(h, s, v))
    while len(out) < k:
        out.append(out[-1])
    return out


def _base_field(art_img, size, rng):
    """A smooth field built from the cover's palette — graphic, not photographic.

    Blobs of the extracted colours are scattered on a coarse grid and
    interpolated up, so the result carries the record's colour identity with
    none of its imagery.
    """
    from PIL import Image
    W, H = size
    palette = extract_palette(art_img, rng)

    small = np.zeros((BASE_GRID, BASE_GRID, 3), dtype=np.float32)
    # Each palette colour claims a centre; every cell takes the weighted blend
    # of the centres nearest it, which gives soft, organic regions.
    centres = [(float(rng.random()) * BASE_GRID,
                float(rng.random()) * BASE_GRID) for _ in palette]
    for y in range(BASE_GRID):
        for x in range(BASE_GRID):
            wsum, acc = 0.0, np.zeros(3, dtype=np.float32)
            for (cx, cy), col in zip(centres, palette):
                d = ((x - cx) ** 2 + (y - cy) ** 2) ** 0.5 + 0.6
                w = 1.0 / (d ** 2.2)
                acc += np.asarray(col, dtype=np.float32) * w
                wsum += w
            small[y, x] = acc / max(wsum, _EPS)

    big = Image.fromarray((np.clip(small, 0, 1) * 255).astype(np.uint8)) \
               .resize((W, H), Image.BICUBIC)
    return np.asarray(big, dtype=np.float32) / 255.0


def _renormalise(f):
    """Pull the field back into range.

    Several transforms accumulate with np.maximum, which reliably clips to
    white once they layer. Rescaling against a high percentile keeps the
    contrast they create without letting the picture bleach out.
    """
    hi = float(np.percentile(f, 99.0))
    lo = float(np.percentile(f, 1.0))
    if hi - lo < 0.15:                      # already flat; leave it alone
        return np.clip(f, 0, 1)
    out = (f - lo) / (hi - lo)
    return np.clip(out * 0.94 + 0.03, 0, 1)


# ── displacement helpers ──────────────────────────────────────────────────────

def _sample(field, xs, ys):
    """Nearest-neighbour sample of `field` at (possibly warped) coordinates."""
    H, W = field.shape[:2]
    xi = np.clip(np.rint(xs).astype(np.int32), 0, W - 1)
    yi = np.clip(np.rint(ys).astype(np.int32), 0, H - 1)
    return field[yi, xi]


def _grid(size):
    W, H = size
    ys, xs = np.mgrid[0:H, 0:W].astype(np.float32)
    return xs, ys


# ── the transforms ────────────────────────────────────────────────────────────

def _t_wave(f, amp, rng, size):
    """Deep bass — long, slow sinusoidal swell."""
    W, H = size
    xs, ys = _grid(size)
    freq = 1.0 + 2.0 * rng.random()
    phase = rng.random() * 6.283
    dx = np.sin(ys / H * freq * 6.283 + phase) * (0.10 * W * amp)
    dy = np.cos(xs / W * freq * 6.283 + phase) * (0.05 * H * amp)
    return _sample(f, xs + dx, ys + dy)


def _t_ripple(f, amp, rng, size):
    """Shimmering synths — concentric ripples from an off-centre origin."""
    W, H = size
    xs, ys = _grid(size)
    cx = W * (0.3 + 0.4 * rng.random())
    cy = H * (0.3 + 0.4 * rng.random())
    r = np.sqrt((xs - cx) ** 2 + (ys - cy) ** 2) + _EPS
    wl = 26.0 + 60.0 * rng.random()
    d = np.sin(r / wl) * (18.0 * amp)
    return _sample(f, xs + d * (xs - cx) / r, ys + d * (ys - cy) / r)


def _t_slice(f, amp, rng, size):
    """Punchy drums — hard rectangular bands kicked sideways."""
    W, H = size
    out = f.copy()
    bands = int(6 + 16 * rng.random())
    for i in range(bands):
        y0 = int(rng.random() * H)
        h = int(H / bands * (0.4 + rng.random()))
        y1 = min(H, y0 + h)
        shift = int((rng.random() - 0.5) * 0.28 * W * amp)
        if y1 > y0 and shift:
            out[y0:y1] = np.roll(f[y0:y1], shift, axis=1)
    return out


def _t_shards(f, amp, rng, size):
    """Percussive tribal — angular triangular displacement."""
    W, H = size
    xs, ys = _grid(size)
    k = 5.0 + 9.0 * rng.random()
    tri = 2.0 * np.abs((xs / W * k) % 1.0 - 0.5) - 0.5
    tri2 = 2.0 * np.abs((ys / H * k) % 1.0 - 0.5) - 0.5
    return _sample(f, xs + tri2 * 0.20 * W * amp, ys + tri * 0.14 * H * amp)


def _t_bloom(f, amp, rng, size):
    """Airy vocals — soft light bloom lifted over the field."""
    from PIL import Image, ImageFilter
    img = Image.fromarray((np.clip(f, 0, 1) * 255).astype(np.uint8))
    blur = img.filter(ImageFilter.GaussianBlur(14 + 30 * amp))
    b = np.asarray(blur, dtype=np.float32) / 255.0
    return 1.0 - (1.0 - f) * (1.0 - b * min(0.85, 0.45 * amp))   # screen


def _t_tear(f, amp, rng, size):
    """Raw distorted — posterise, then tear the channels apart."""
    levels = max(3, int(9 - 5 * min(amp, 1.6)))
    q = np.floor(f * levels) / max(1, levels - 1)
    W = size[0]
    sh = int(0.03 * W * amp) + 1
    q[..., 0] = np.roll(q[..., 0], sh, axis=1)
    q[..., 2] = np.roll(q[..., 2], -sh, axis=1)
    return np.clip(q, 0, 1)


def _t_grain(f, amp, rng, size):
    """Acoustic wood — directional fibre grain."""
    W, H = size
    n = rng.normal(0.0, 1.0, (H, 1)).astype(np.float32)
    n = n + rng.normal(0.0, 0.35, (H, W)).astype(np.float32)
    return np.clip(f + n[..., None] * 0.055 * amp, 0, 1)


def _t_halation(f, amp, rng, size):
    """Warm analog — horizontal bleed plus scanlines."""
    W, H = size
    bleed = f.copy()
    for s in (2, 5, 9):
        bleed = np.maximum(bleed, np.roll(f, int(s * amp) + 1, axis=1) * 0.85)
    ys = np.arange(H, dtype=np.float32)[:, None, None]
    scan = 0.94 + 0.06 * np.sin(ys / 2.4)
    out = bleed * scan
    out[..., 0] *= 1.05
    out[..., 2] *= 0.95
    return np.clip(out, 0, 1)


def _t_quantise(f, amp, rng, size):
    """Crisp digital — chunky nearest-neighbour pixelation."""
    W, H = size
    block = max(4, int(8 + 26 * amp))
    small_h, small_w = max(2, H // block), max(2, W // block)
    ys = (np.arange(H) * small_h // H).clip(0, small_h - 1)
    xs = (np.arange(W) * small_w // W).clip(0, small_w - 1)
    coarse = f[(np.arange(small_h) * H // small_h)][:, (np.arange(small_w) * W // small_w)]
    return coarse[ys][:, xs]


def _t_streak(f, amp, rng, size):
    """Clean electric — long directional smear."""
    W, H = size
    axis = 1 if rng.random() < 0.5 else 0
    out = f.copy()
    steps = int(6 + 18 * amp)
    for i in range(1, steps + 1):
        out = np.maximum(out, np.roll(f, i * 3, axis=axis) * (1.0 - i / (steps + 2.0)))
    return np.clip(out, 0, 1)


TRANSFORM_FNS = {
    "wave": _t_wave, "ripple": _t_ripple, "slice": _t_slice,
    "shards": _t_shards, "bloom": _t_bloom, "tear": _t_tear,
    "grain": _t_grain, "halation": _t_halation,
    "quantise": _t_quantise, "streak": _t_streak,
}


# ── colour + staging ──────────────────────────────────────────────────────────

def _apply_mood(f, mood):
    sat, con, hue, lift = MOOD_COLOUR.get((mood or "").strip().lower(),
                                          (1.0, 1.0, 0.0, 0.0))
    lum = f @ np.array([0.299, 0.587, 0.114], dtype=np.float32)
    out = lum[..., None] + (f - lum[..., None]) * sat          # saturation
    out = (out - 0.5) * con + 0.5                              # contrast
    out = out + lift                                           # lift/crush
    if abs(hue) > 1e-3:                                        # cheap hue turn
        r, g, b = out[..., 0], out[..., 1], out[..., 2]
        out = np.stack([r + hue * (g - b), g + hue * (b - r), b + hue * (r - g)], -1)
    return np.clip(out, 0, 1)


def _apply_staging(f, staging, rng, size):
    W, H = size
    xs, ys = _grid(size)
    if staging == "mirror":
        half = f[:, : W // 2]
        return np.concatenate([half, half[:, ::-1]], axis=1)[:, :W]
    if staging == "warm_vignette":
        cx, cy = W / 2.0, H * 0.42
        r = np.sqrt(((xs - cx) / W) ** 2 + ((ys - cy) / H) ** 2)
        v = np.clip(1.15 - 1.5 * r, 0.18, 1.0)[..., None]
        out = f * v
        out[..., 0] = np.clip(out[..., 0] * 1.10, 0, 1)
        return np.clip(out, 0, 1)
    if staging == "horizon":
        band = np.clip(1.25 - np.abs(ys / H - 0.55) * 3.2, 0.25, 1.0)[..., None]
        return np.clip(f * band, 0, 1)
    if staging == "smear":
        out = f.copy()
        for i in range(1, 10):
            out = np.maximum(out, np.roll(f, i * 5, axis=1) * (1 - i / 12.0))
        return np.clip(out * 1.05, 0, 1)
    if staging == "crush":
        return np.clip((f - 0.5) * 1.5 + 0.38, 0, 1)
    if staging == "haze":
        from PIL import Image, ImageFilter
        img = Image.fromarray((np.clip(f, 0, 1) * 255).astype(np.uint8))
        b = np.asarray(img.filter(ImageFilter.GaussianBlur(22)), np.float32) / 255.0
        return np.clip(f * 0.45 + b * 0.55 + 0.06, 0, 1)
    # default: gentle vignette so type always has somewhere to sit
    r = np.sqrt(((xs - W / 2) / W) ** 2 + ((ys - H / 2) / H) ** 2)
    return np.clip(f * np.clip(1.12 - 0.85 * r, 0.3, 1.0)[..., None], 0, 1)


# ── entry point ───────────────────────────────────────────────────────────────

def generate(art_img, size, labels, track_id):
    """Return a PIL RGB image: the track's abstract cover.

    `labels` is the dict already carried on a track — energy/mood/texture/feel.
    """
    from PIL import Image
    rng = _rng(track_id)
    labels = labels or {}

    amp = ENERGY_AMPLITUDE.get((labels.get("energy") or "").strip().lower(), 1.0)
    field = _base_field(art_img, size, rng)

    # texture → up to two layered transforms, in the order the labeller wrote
    names = [TEXTURE_TRANSFORMS[t] for t in _tokens(labels.get("texture"))
             if t in TEXTURE_TRANSFORMS][:2]
    if not names:                       # unlabelled track still gets a picture
        names = [list(TRANSFORM_FNS)[int(rng.integers(len(TRANSFORM_FNS)))]]
    for n in names:
        field = TRANSFORM_FNS[n](field, amp, rng, size)
        field = _renormalise(field)

    field = _apply_mood(field, labels.get("mood"))
    field = _apply_staging(
        field, FEEL_STAGING.get((labels.get("feel") or "").strip().lower()),
        rng, size)

    return Image.fromarray((np.clip(field, 0, 1) * 255).astype(np.uint8)), names
