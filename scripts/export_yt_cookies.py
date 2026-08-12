#!/usr/bin/env python3
"""Export YouTube cookies from Chrome to .yt_cookies.txt — run BY HAND, once.

This is the only code in the repo allowed to read Chrome's cookie store:
decrypting it asks macOS Keychain for the "Chrome Safe Storage" key, which
pops a password prompt for every fresh process. Run interactively and click
"Always Allow" so it never asks again; everything scheduled (ig_cron.sh's
resolve stage) reads only the exported file via lib/ig_audio.COOKIE_FILE.

yt-dlp refreshes the file's cookies after each run it uses them in, so one
export keeps working for as long as the pipeline keeps running. If YouTube
ever invalidates the session outright (password change, logout), just run
this again.

    python3 scripts/export_yt_cookies.py
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from lib.ig_audio import COOKIE_FILE  # noqa: E402


def main():
    import yt_dlp
    with yt_dlp.YoutubeDL({"quiet": True, "no_warnings": True,
                           "cookiesfrombrowser": ("chrome",),
                           "cookiefile": COOKIE_FILE}) as ydl:
        jar = ydl.cookiejar
        n = sum(1 for c in jar if "youtube.com" in (c.domain or "")
                or "google.com" in (c.domain or ""))
        jar.save()
    os.chmod(COOKIE_FILE, 0o600)
    print(f"wrote {COOKIE_FILE} ({n} youtube/google cookies)")
    if not n:
        print("WARNING: no YouTube cookies found — is Chrome logged in?")


if __name__ == "__main__":
    main()
