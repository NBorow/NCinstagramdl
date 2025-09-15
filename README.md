# Social Export Tool

A menu-driven tool for managing Instagram data exports (“profile dumps”) and processing media. Supports manual, persistent Chrome login with cookie export, optional auto-login with fallback, and DM workflows.

## Features
- Scan and select Instagram **profile dumps** (unzipped exports)
- **DM download**: browse conversations, fetch shared posts (and optionally shared profiles)
- **Saved posts download**: download posts from your saved collections and unsorted saved posts
- **Liked posts download**: download posts you've liked
- Cookie management: **manual login** with a persistent Chrome profile → exports a Netscape cookie file for yt-dlp/gallery-dl
- Safety presets and pacing (human-like delays, long breaks)
- Local SQLite database of downloaded items for dedupe/stats

## Requirements
- **Python 3.8+**
- **Google Chrome** installed (driver handled automatically by `webdriver-manager`)
- Python packages:
  ```sh
  pip install requests beautifulsoup4 lxml tqdm pytz dateparser emoji chardet python-dateutil selenium webdriver-manager yt-dlp gallery-dl
  ```
- Optional but recommended: **ffmpeg** on PATH (for media merges)

The app checks for ffmpeg at startup and strongly recommends installing it. Without ffmpeg, some downloads may skip merging/transcoding and can fail depending on format.

## Installation
```sh
git clone <your-repo-url>
# Repo name uses hyphen, Python entry script uses underscore
cd social_export_tool
# If you keep a requirements.txt, use it; otherwise install packages from the command above.
```

## Configuration
Create `config.txt` in the project root. Minimum:

```
PROFILE_DUMP_DIRECTORY=C:\path\to\unzipped\instagram_dump
DOWNLOAD_DIRECTORY=C:\path\to\downloads

# Chrome user data directory for persistent login. Use a full path (drive included).
PROFILE_DIR=C:\Users\you\social_export_tool\chrome_profile

# Manual vs automatic login
SAFER_MANUAL_LOGIN=true

# If you set SAFER_MANUAL_LOGIN=false, you may optionally supply credentials:
# USERNAME=your_instagram_username
# PASSWORD=your_instagram_password

# Auto-retry when Instagram rate-limits you (HTTP 429 / "Please wait a few minutes")
AUTO_RETRY_ON_RATE_LIMIT=true

# Optional: Ask to append DM send messages to filenames
# When enabled, detects text messages sent <1s after sharing posts in DMs
# and offers to append them as filename suffixes (e.g., CrAb1234_cute_message.mp4)
ASK_FOR_SEND_MESSAGE_APPEND=false

# Prepend post publish date to filenames (off by default)
# Resulting pattern:
#   YYYYMMDD_HHMM_<shortcode>_by_<owner>_<caption_snippet>.%(ext)s
APPEND_POST_DATE=false
```

#

#### Completely manual cookie import (recommended)
**Safest method**: Already logged in on your normal browser? Export **web cookies** for `instagram.com` in **Netscape format** (via a cookie exporter extension) and place the file where the app expects it (e.g., `./cookies/insta_cookies.txt`). This avoids Selenium automation entirely and uses your existing, trusted session. Keep it **per-account** and treat the file like a password.

**Which cookies matter?** Must-haves: `sessionid`, `ds_user_id`, `csrftoken`. Nice-to-haves: `mid`, `ig_did`, `rur`, `shbid`, `shbts`, `ig_nrcb`.
## Login and Cookies
- **Manual (default)**  
  - The app opens Chrome with your `PROFILE_DIR` (a real, persistent user data dir).  
  - You log in by hand (and complete any 2FA/checkpoints).  
  - The app exports cookies to a **Netscape** file used by yt-dlp/gallery-dl (typically `./cookies/insta_cookies.txt`).  
  - On next runs, if cookies are still valid, login is skipped.

- **Automatic (only if `SAFER_MANUAL_LOGIN=false`)**  
  - The app uses your `USERNAME`/`PASSWORD` to attempt login **up to 3 times**.  
  - If it still can’t obtain valid cookies, it falls back to the **manual** flow described above.

#### Credential prompts (automatic mode behavior)
- **Both** `USERNAME` and `PASSWORD` present → try them. On failure, prompt again as needed.  
- **Only** `USERNAME` present → prompt **only for password**.  
- **Only** `PASSWORD` present → treated as missing creds → prompt for **both**.  
- **Neither** present → prompt for **both**.

### Safety Presets
Open **Settings** from the app and apply a preset:

- `super_duper_schizo_safe`: forces manual login; most conservative pacing and caps
- `super_safe`: forces manual login; conservative pacing
- Other presets: standard/riskier pacing (manual login still recommended)

You can also edit individual values (e.g., toggle `SAFER_MANUAL_LOGIN`, change `PROFILE_DIR`).

## Using Instagram Data Exports
1. Visit the Instagram Data Download page: https://accountscenter.meta.com/info_and_permissions/dyi  
2. Request your data, wait for the email, download the ZIP.  
3. Unzip into `PROFILE_DUMP_DIRECTORY`.

## Running
```sh
python social_export_tool.py
```
- Choose a profile dump.  
- Use the options menu (e.g., **DM Download**) when available.  
- Navigation: number to select, `n`/`p` to page, `c` for Settings, `q` to quit.

## What Gets Downloaded
- **DM Download**: downloads shared posts in selected conversations. Profile shares can optionally trigger full profile grabs (depending on options shown in-app).
- **Saved Posts Download**: downloads posts from your saved collections and unsorted saved posts, organized into per-collection folders under `downloads/saved/<CollectionName>/` with unsorted posts going to `downloads/saved/_unsorted/`
- **Liked Posts Download**: downloads posts you've liked, organized under `downloads/liked/`
- **Profile Posts Download**: downloads all posts from a specific user's profile (not yet implemented)

### DM Send Message Classification
When `ASK_FOR_SEND_MESSAGE_APPEND=true` is set in config.txt, the app can detect and use "send messages" from DM conversations to classify downloaded posts:

- **Send Message Detection**: Looks for human text messages sent by the same person within 1 second of sharing an Instagram post
- **Filename Enhancement**: If detected, prompts once per DM thread to append these messages as filename suffixes
- **Example**: A post shared with "cute!" sent immediately after becomes `CrAb1234_cute.mp4` instead of `CrAb1234.mp4`
- **Use Cases**: Useful for organizing posts by context, reactions, or comments made when sharing in conversations
- **Safety**: Only affects DM downloads, preserves original Instagram captions, and converts text to safe filename characters

Filenames are ASCII-safe and length-capped. If APPEND_POST_DATE=true, filenames are prefixed with the post's publish datetime (YYYYMMDD_HHMM_…). This does not change database records—only the on-disk name.

## Cookies and Downloader Integration
- Cookies are exported in **Netscape format** and reused by **yt-dlp** / **gallery-dl**.  
- The downloader calls remain unchanged and read the same cookie file every run.  
- The persistent Chrome profile in `PROFILE_DIR` stabilizes device fingerprint and reduces checkpoints.

## Database
- Creates a local SQLite DB (e.g., `downloaded_posts.db`) to record each item (shortcode, URL, source such as dm/saved/liked/profile, status, timestamps, etc.).  
- Summaries/stats are printed after runs.  
- Safe to keep between runs for dedupe.

## Session Summary & Logs
On clean exit or Ctrl-C, the app prints a session summary (attempts, successes, failures, skips, rate-limit/checkpoint counts, success rate).

Console output is tee'd to a run log in the configured log directory (path is printed at startup). Failures are also appended to total_failures.log.

Use these logs to resume work, inspect errors, or post-process (e.g., build sorted "views" from DB metadata).

## Safety and Pacing
- Human-like per-request delays, periodic long breaks, and backoff on errors.  
- During waits, if you see "(Press Enter to skip)", you can press Enter to skip on Windows and Linux/macOS (POSIX TTYs). In non-interactive runs (cron/systemd/nohup), the prompt is suppressed and waits are non-skippable.


### Blocking & Recovery
- **RateLimitError** (429 / "Please wait a few minutes" / temporary block):
If AUTO_RETRY_ON_RATE_LIMIT=true, the app automatically retries the same item using an exponential schedule with ±15% jitter (e.g., ~75s → ~150s → ~300s → …, capped). Jitter avoids synchronized retry bursts. If set to false, you'll get an interactive prompt (retry / delayed retry / skip / quit).

- **LoginRequiredError** ("login required", "not logged in"):
  Cookies/session are invalid or expired. Use **manual login now** (persistent Chrome profile) or refresh your cookies, then retry.

- **CheckpointError** ("verify it's you", `challenge_required`):
  Complete manual verification in the persistent profile, then **wait ~30–60 minutes** before resuming, or switch accounts/profiles. Prompt offers: retry / manual-login-now / skip / quit.

## Notes
- Respect Instagram’s Terms and local laws.  
- Keep `profiles/` and `cookies/` out of version control.  
- `PROFILE_DIR` accepts absolute paths with drives (Windows) or absolute POSIX paths. If omitted, the app falls back to a sensible in-repo default.
