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

from lib.ig_audio import COOKIE_FILE, CHROME_PROFILE, refresh_cookie_file  # noqa: E402


def main():
    import argparse
    ap = argparse.ArgumentParser()
    # This Mac has 15 Chrome profiles and profile-less extraction does NOT
    # pick "Default" — it silently read Profile 1, whose YouTube session is a
    # different channel (@tommasosaggini5256, 28 likes) than the same Google
    # account's real one (@TommasoSaggini, 2k+). Explicit, always.
    ap.add_argument("--profile", default=CHROME_PROFILE)
    args = ap.parse_args()

    n = refresh_cookie_file(args.profile)
    print(f"wrote {COOKIE_FILE} ({n} youtube/google cookies from "
          f"Chrome profile {args.profile!r})")
    if not n:
        print("WARNING: no YouTube cookies found — is Chrome logged in?")


if __name__ == "__main__":
    main()
