"""
Measure what a recording actually sounds like.

The text labeller reads "NewJeans — Ditto" and reaches for what k-pop usually
is; measured across the pool that bias is stark (k-pop is labelled "euphoric"
3.7x more than its runner-up, where no mood dominates overall). Numbers taken
from the audio don't have that prior: a hazy, mid-tempo, soft-transient record
measures as one whatever the genre says.

Deliberately built on ffmpeg + numpy/scipy only — no librosa, no new wheels to
install in the prod container. Everything here is derived from one mono decode.
"""
import subprocess

import numpy as np

SR = 22050
_MAX_SECONDS = 150          # analysing the whole of a 9-minute track buys nothing


def _decode(path, sr=SR):
    """Mono float32 PCM via ffmpeg, trimmed to a sane analysis length."""
    proc = subprocess.run(
        ["ffmpeg", "-v", "quiet", "-i", path, "-ac", "1", "-ar", str(sr),
         "-t", str(_MAX_SECONDS), "-f", "s16le", "-"],
        capture_output=True, check=True)
    x = np.frombuffer(proc.stdout, dtype=np.int16).astype(np.float32) / 32768.0
    return x


def _frames(x, win=2048, hop=512):
    n = 1 + max(0, (len(x) - win) // hop)
    if n < 2:
        return np.zeros((0, win), dtype=np.float32)
    idx = np.arange(win)[None, :] + hop * np.arange(n)[:, None]
    return x[idx] * np.hanning(win).astype(np.float32)


def analyse(path, sr=SR):
    """A compact, human-readable description of the sound.

    Returns plain floats/strings so it can go straight into a prompt or a log
    without a serialisation step.
    """
    x = _decode(path, sr)
    if x.size < sr:
        return {}

    fr = _frames(x)
    if not len(fr):
        return {}
    spec = np.abs(np.fft.rfft(fr, axis=1)) + 1e-10
    freqs = np.fft.rfftfreq(fr.shape[1], 1.0 / sr)

    # Loudness and dynamics. Crest factor separates a squashed, mastered-loud
    # record from one that still breathes.
    rms = np.sqrt(np.mean(fr ** 2, axis=1)) + 1e-10
    peak = np.max(np.abs(x))
    crest_db = float(20 * np.log10(peak / (np.sqrt(np.mean(x ** 2)) + 1e-10)))
    loudness_db = float(20 * np.log10(np.mean(rms)))
    # How much the level moves — steady drone vs dynamic arrangement.
    dynamics_db = float(np.percentile(20 * np.log10(rms), 95)
                        - np.percentile(20 * np.log10(rms), 20))

    # Brightness and noisiness.
    centroid = float(np.mean(np.sum(spec * freqs[None, :], axis=1)
                             / np.sum(spec, axis=1)))
    flatness = float(np.mean(np.exp(np.mean(np.log(spec), axis=1))
                             / np.mean(spec, axis=1)))
    cumulative = np.cumsum(spec, axis=1)
    rolloff_idx = np.argmax(cumulative >= 0.85 * cumulative[:, -1:], axis=1)
    rolloff = float(np.mean(freqs[rolloff_idx]))

    # Band balance — bass weight vs air.
    def band(lo, hi):
        m = (freqs >= lo) & (freqs < hi)
        return float(np.mean(np.sum(spec[:, m], axis=1) / np.sum(spec, axis=1)))
    low, mid, high = band(20, 250), band(250, 4000), band(4000, sr / 2)

    # Onsets: spectral flux peaks. Density ≈ how percussive/busy it is, and the
    # median gap between them gives a tempo estimate without a beat tracker.
    flux = np.sqrt(np.sum(np.diff(spec, axis=0).clip(min=0) ** 2, axis=1))
    if flux.size > 8:
        thresh = np.mean(flux) + 0.9 * np.std(flux)
        onsets = np.where(flux > thresh)[0]
        hop_s = 512.0 / sr
        onset_rate = float(len(onsets) / (len(x) / sr))
        gaps = np.diff(onsets) * hop_s
        gaps = gaps[(gaps > 0.15) & (gaps < 2.0)]
        tempo = float(60.0 / np.median(gaps)) if gaps.size > 4 else 0.0
        # Onsets land on subdivisions as often as on the beat, so the raw
        # estimate skews high — a 76 BPM soul record reads as 152. Fold into
        # the range most music actually sits in. Genuinely fast genres
        # (drum & bass) get halved too; the tempo is a hint, not a claim.
        while tempo and tempo < 70:
            tempo *= 2
        while tempo >= 145:
            tempo /= 2
    else:
        onset_rate, tempo = 0.0, 0.0

    # Transient sharpness — snappy drums vs a wash of pads.
    attack = float(np.mean(np.diff(rms).clip(min=0)) / (np.mean(rms) + 1e-10))

    return {
        "tempo_bpm": round(tempo, 1),
        "loudness_db": round(loudness_db, 1),
        "dynamic_range_db": round(dynamics_db, 1),
        "crest_factor_db": round(crest_db, 1),
        "brightness_hz": round(centroid, 0),
        "rolloff_hz": round(rolloff, 0),
        "noisiness": round(flatness, 4),
        "low_band": round(low, 3),
        "mid_band": round(mid, 3),
        "high_band": round(high, 3),
        "onsets_per_sec": round(onset_rate, 2),
        "transient_sharpness": round(attack, 4),
    }


def describe(f):
    """Turn the measurements into short English for a prompt.

    A model reasons better about "slow, 78 BPM, very dark, smooth and sustained"
    than about a dict of floats — and this keeps the numbers alongside it so
    nothing is lost in the translation.
    """
    # Cut points come from measuring a real sample rather than intuition — the
    # first guesses put every track in "noisy/distorted" and "densely
    # percussive", which describes nothing. Sample is small (~a dozen tracks),
    # so these are calibrated-but-provisional; widen them as the pool grows.
    if not f:
        return "no audio analysis available"
    bits = []
    t = f.get("tempo_bpm") or 0
    if t:
        pace = ("very slow" if t < 75 else "slow" if t < 95 else
                "mid-tempo" if t < 115 else "upbeat" if t < 132 else "fast")
        bits.append(f"{pace} (~{t:.0f} BPM)")
    b = f.get("brightness_hz") or 0
    bits.append("very dark and warm" if b < 1400 else "dark" if b < 1900 else
                "balanced" if b < 2400 else "bright" if b < 2800 else
                "very bright and airy")
    dr = f.get("dynamic_range_db") or 0
    bits.append("compressed and constant" if dr < 8 else
                "moderately dynamic" if dr < 11 else
                "dynamic, lots of light and shade")
    n = f.get("noisiness") or 0
    bits.append("clean and tonal" if n < 0.22 else
                "some grit" if n < 0.36 else "fuzzy, noisy texture")
    o = f.get("onsets_per_sec") or 0
    bits.append("sparse, unhurried" if o < 5.2 else
                "moderately busy" if o < 7.0 else "dense, busy rhythm")
    a = f.get("transient_sharpness") or 0
    bits.append("soft, washed attacks" if a < 0.050 else
                "defined transients" if a < 0.070 else "sharp, snappy transients")
    lb, hb = f.get("low_band") or 0, f.get("high_band") or 0
    if lb > 0.30:
        bits.append("bass-heavy")
    if hb > 0.22:
        bits.append("lots of top-end air")
    elif hb < 0.10:
        bits.append("little top end, muffled")
    return "; ".join(bits)
