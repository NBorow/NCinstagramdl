# NCInstagramDL

A menu-driven tool for managing Instagram data exports (“profile dumps”) and downloading media. Supports manual, persistent Chrome login with cookie export, optional auto-login with fallback, and DM download workflows.

## Features
- Scan and select Instagram **profile dumps** (unzipped exports)
- **DM download**: browse conversations, fetch shared posts (and optionally shared profiles)
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

## Installation
```sh
git clone <your-repo-url>
cd NCInstagramDL
# If you keep a requirements.txt, use it; otherwise install packages from the command above.
```

## Configuration
Create `config.txt` in the project root. Minimum:

```
PROFILE_DUMP_DIRECTORY=C:\path\to\unzipped\instagram_dump
DOWNLOAD_DIRECTORY=C:\path\to\downloads

# Chrome user data directory for persistent login. Use a full path (drive included).
PROFILE_DIR=C:\Users\you\NCInstagramDL\chrome_profile

# Manual vs automatic login
SAFER_MANUAL_LOGIN=true

# If you set SAFER_MANUAL_LOGIN=false, you may optionally supply credentials:
# USERNAME=your_instagram_username
# PASSWORD=your_instagram_password
```

### Login and Cookies
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
1. Visit the Instagram Data Download page: https://accountscenter.instagram.com/info_and_permissions/dyi/  
2. Request your data, wait for the email, download the ZIP.  
3. Unzip into `PROFILE_DUMP_DIRECTORY`.

## Running
```sh
python ncinstagramdl.py
```
- Choose a profile dump.  
- Use the options menu (e.g., **DM Download**) when available.  
- Navigation: number to select, `n`/`p` to page, `c` for Settings, `q` to quit.

## What Gets Downloaded
- **DM Download**: downloads shared posts in selected conversations. Profile shares can optionally trigger full profile grabs (depending on options shown in-app).
- Top-level **Profile / Liked / Saved** entries may indicate “not yet implemented” if those workflows are pending.

## Cookies and Downloader Integration
- Cookies are exported in **Netscape format** and reused by **yt-dlp** / **gallery-dl**.  
- The downloader calls remain unchanged and read the same cookie file every run.  
- The persistent Chrome profile in `PROFILE_DIR` stabilizes device fingerprint and reduces checkpoints.

## Database
- Creates a local SQLite DB (e.g., `downloaded_posts.db`) to record each item (shortcode, URL, source such as dm/saved/liked/profile, status, timestamps, etc.).  
- Summaries/stats are printed after runs.  
- Safe to keep between runs for dedupe.

## Safety and Pacing
- Human-like per-request delays, periodic long breaks, and backoff on errors.  
- On Windows, you can press Enter to skip a long break if a prompt indicates it.

## Troubleshooting
- **Login stuck/challenge**: use manual mode (`SAFER_MANUAL_LOGIN=true`), ensure `PROFILE_DIR` points to a writable, persistent folder. Log in once, then re-run.  
- **Switching accounts (single-profile mode)**: log out inside Chrome, log in as the new account, export cookies again. If you want a clean slate, move/clear `PROFILE_DIR`.  
- **Driver issues**: make sure Chrome is installed; `webdriver-manager` will fetch a matching driver.  
- **Rate limits**: the script slows down automatically; consider safer presets if you hit blocks.

## Notes
- Respect Instagram’s Terms and local laws.  
- Keep `profiles/` and `cookies/` out of version control.  
- `PROFILE_DIR` accepts absolute paths with drives (Windows) or absolute POSIX paths. If omitted, the app falls back to a sensible in-repo default.
