import os
import re
import getpass
import json
import time
import subprocess
import unicodedata
import random
import shutil
import sys
import select
import threading
import signal
from collections import deque
from datetime import datetime
from glob import glob
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import requests

# Import database functions
from db import init_db, is_downloaded, get_post, record_download, record_failure, get_download_stats, close_db, get_recent_download_timestamps

# --- Pre-flight checks ---
def check_ffmpeg_availability():
    """
    Check if ffmpeg is available on the system.
    Returns True if found, False otherwise.
    """
    try:
        # Check if ffmpeg is available in PATH
        result = subprocess.run(['ffmpeg', '-version'], 
                              capture_output=True, text=True, timeout=5)
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False

def print_ffmpeg_warning():
    """
    Print a strong recommendation to install ffmpeg.
    """
    print("\n" + "="*80)
    print("⚠️  FFMPEG NOT FOUND ⚠️")
    print("="*80)
    print("FFmpeg is not installed or not available in your system PATH.")
    print()
    print("FFmpeg is STRONGLY RECOMMENDED for:")
    print("• Merging video and audio streams from Instagram posts")
    print("• Converting media formats for better compatibility")
    print("• Handling complex media downloads")
    print()
    print("Installation options:")
    print("• Windows: Download from https://ffmpeg.org/download.html")
    print("• macOS: brew install ffmpeg")
    print("• Ubuntu/Debian: sudo apt install ffmpeg")
    print("• CentOS/RHEL: sudo yum install ffmpeg")
    print()
    print("After installation, restart this program.")
    print("="*80)
    print()

# --- Session tracking for summary ---
class SessionTracker:
    def __init__(self):
        self.start_time = time.time()
        self.downloads_attempted = 0
        self.downloads_successful = 0
        self.downloads_failed = 0
        self.downloads_skipped = 0
        self.rate_limits_hit = 0
        self.checkpoints_hit = 0
        self.login_required_hits = 0
        self.errors = []
    
    def record_download_attempt(self):
        self.downloads_attempted += 1
    
    def record_download_success(self):
        self.downloads_successful += 1
    
    def record_download_failure(self):
        self.downloads_failed += 1
    
    def record_download_skip(self):
        self.downloads_skipped += 1
    
    def record_rate_limit(self):
        self.rate_limits_hit += 1
    
    def record_checkpoint(self):
        self.checkpoints_hit += 1
    
    def record_login_required(self):
        self.login_required_hits += 1
    
    def record_error(self, error_msg):
        self.errors.append(error_msg)
    
    def get_session_summary(self):
        """Generate a comprehensive session summary."""
        duration = time.time() - self.start_time
        hours = int(duration // 3600)
        minutes = int((duration % 3600) // 60)
        seconds = int(duration % 60)
        
        summary = []
        summary.append("\n" + "="*60)
        summary.append("📊 SESSION SUMMARY")
        summary.append("="*60)
        summary.append(f"Session duration: {hours:02d}:{minutes:02d}:{seconds:02d}")
        summary.append("")
        summary.append("Download Statistics:")
        summary.append(f"  • Attempted: {self.downloads_attempted}")
        summary.append(f"  • Successful: {self.downloads_successful}")
        summary.append(f"  • Failed: {self.downloads_failed}")
        summary.append(f"  • Skipped: {self.downloads_skipped}")
        
        if self.downloads_attempted > 0:
            success_rate = (self.downloads_successful / self.downloads_attempted) * 100
            summary.append(f"  • Success rate: {success_rate:.1f}%")
        
        summary.append("")
        summary.append("Rate Limiting Events:")
        summary.append(f"  • Rate limits hit: {self.rate_limits_hit}")
        summary.append(f"  • Checkpoints encountered: {self.checkpoints_hit}")
        summary.append(f"  • Login required events: {self.login_required_hits}")
        
        if self.errors:
            summary.append("")
            summary.append("Errors encountered:")
            for error in self.errors[-5:]:  # Show last 5 errors
                summary.append(f"  • {error}")
            if len(self.errors) > 5:
                summary.append(f"  • ... and {len(self.errors) - 5} more errors")
        
        summary.append("="*60)
        return "\n".join(summary)

# Global session tracker
SESSION_TRACKER = SessionTracker()

# --- Shutdown + cancelable sleep helpers ---
SHUTDOWN = threading.Event()
_SIGINT_COUNT = 0

def _signal_handler(signum, frame):
	global _SIGINT_COUNT
	_SIGINT_COUNT += 1
	if _SIGINT_COUNT == 1:
		print("\n[CTRL-C] Received. Finishing current item, then exiting cleanly... (press Ctrl-C again to force quit)")
		SHUTDOWN.set()
	else:
		print("\n[CTRL-C] Forcing exit now.")
		try:
			os._exit(130)
		except Exception:
			raise SystemExit(130)

def install_signal_handlers():
	try:
		signal.signal(signal.SIGINT, _signal_handler)
		signal.signal(signal.SIGTERM, _signal_handler)
	except Exception:
		# On some platforms (e.g., older Windows), SIGTERM may not be available.
		pass

def sleep_with_cancel(seconds: float) -> bool:
	"""
	Sleep up to `seconds`; wake early if SHUTDOWN is set.
	Returns True if a shutdown was requested during the wait.
	"""
	if seconds <= 0:
		return SHUTDOWN.is_set()
	end = time.time() + seconds
	while not SHUTDOWN.is_set():
		remaining = end - time.time()
		if remaining <= 0:
			break
		SHUTDOWN.wait(min(remaining, 0.5))  # short slices keep UI responsive
	return SHUTDOWN.is_set()

def posix_sleep_with_optional_enter(seconds: float, msg: str) -> bool:
	"""
	POSIX-only: sleep up to `seconds`, but if stdin is a TTY, allow Enter to skip.
	Returns True if a shutdown was requested during the wait, False otherwise.
	"""
	if seconds <= 0:
		return SHUTDOWN.is_set()

	# If not interactive, just do a cancellable sleep with no hint.
	if not sys.stdin.isatty():
		return sleep_with_cancel(seconds)

	# Interactive TTY: show a hint and allow Enter to skip via select()
	print(f"{msg} (Press Enter to skip)")
	end = time.time() + seconds
	while not SHUTDOWN.is_set():
		remaining = end - time.time()
		if remaining <= 0:
			break
		# Wait up to 0.5s slices, or the remaining time, whichever is smaller
		timeout = min(0.5, max(0.0, remaining))
		r, _, _ = select.select([sys.stdin], [], [], timeout)
		if r:
			try:
				# Consume the line and treat it as "skip"
				sys.stdin.readline()
			except Exception:
				pass
			print("[SAFE] Skip requested.")
			break
	return SHUTDOWN.is_set()

# --- Caption normalization helpers ---
def _mojibake_candidate(s: str) -> bool:
	return any('\u0080' <= ch <= '\u00FF' for ch in s)

def _looks_much_better(a: str, b: str) -> bool:
	bad = "ÃÂØð¢¬¤§"
	score = lambda s: (sum(ch.isalnum() for ch in s) - sum(ch in bad for ch in s))
	return score(a) > score(b)

def repair_mojibake(s: str) -> str:
	if not s: return s
	if _mojibake_candidate(s):
		try:
			cand = s.encode('latin-1','ignore').decode('utf-8','ignore')
			if _looks_much_better(cand, s):
				return cand
		except Exception:
			pass
	return s

def normalize_caption_text(s: str) -> str:
    if not s:
        return s
    # Existing steps
    s = repair_mojibake(s)
    s = unicodedata.normalize("NFKC", s)
    # strip zero-width/control chars, collapse spaces
    s = ''.join(ch if (ch >= ' ' and ch not in '\u200b\u200c\u200d\u2060') else ' ' for ch in s)
    s = re.sub(r'\s+', ' ', s).strip()
    # NEW: force ASCII only (drop emojis and non-Latin)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s

def clean_text_for_filename(s: str, max_len: int | None = None) -> str:
	"""
	ASCII-only captions for filenames:
	normalize -> ASCII-only -> collapse whitespace -> sanitize -> truncate.
	"""
	if not s:
		return s
	s = normalize_caption_text(s)
	s = sanitize_filename(s)  # keep your existing sanitizer unchanged
	if max_len and len(s) > max_len:
		s = s[:max_len-1] + '…'
	return s

# --- Exception classes for recoverable errors ---
class RateLimitError(Exception): pass
class CheckpointError(Exception): pass
class LoginRequiredError(Exception): pass

# --- Rate limit configuration ---
RATE_LIMIT_SCHEDULE = [75, 150, 300, 600, 1200, 2400, 4800]  # seconds

def get_jittered_delay(base: int | float, jitter: float = 0.15) -> float:
    """
    Add jitter to a delay to make it more human-like.
    
    Args:
        base: Base delay in seconds
        jitter: Percentage of jitter to add (0.0 to 1.0), default 0.15 (15%)
        
    Returns:
        float: Jittered delay in seconds, rounded to 1 decimal place
    """
    # ±15% jitter by default
    low = max(0, base * (1 - jitter))
    high = base * (1 + jitter)
    return round(random.uniform(low, high), 1)

def parse_bool(s: str, default: bool) -> bool:
	if s is None: return default
	return s.strip().lower() in ("1","true","yes","y","on")



def classify_block_reason(stderr: str):
    if not stderr:
        return None
    s = stderr.lower()
    if any(p in s for p in ["checkpoint", "challenge_required", "verify it's you", "verify its you"]):
        return "checkpoint"
    if any(p in s for p in ["login required", "please log in", "not logged in"]):
        return "login_required"
    if any(p in s for p in ["429", "rate limit", "please wait", "try again later", "temporarily blocked"]):
        return "rate_limit"
    return None

CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'config.txt')
COOKIE_FILE = os.path.join(os.path.dirname(__file__), 'insta_cookies.txt')
PAGE_SIZE = 10

# --- Centralized Chrome options builder ---
def build_chrome_options(profile_dir: str, window_size: str = "1280,900") -> Options:
    """
    Build standardized Chrome options with minimal flags and persistent profile.
    
    Args:
        profile_dir: Directory for persistent Chrome profile
        window_size: Window size as "width,height" string
        
    Returns:
        Options: Configured Chrome options
    """
    opts = Options()
    opts.add_argument(f"--user-data-dir={os.path.abspath(profile_dir)}")
    opts.add_argument(f"--window-size={window_size}")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--disable-blink-features=AutomationControlled")
    return opts

# --- Config parsing helpers for manual login ---
def normalize_profile_dir(raw: str) -> str:
	script_dir = os.path.dirname(__file__)
	if not raw or not raw.strip():
		# default if not provided
		return os.path.join(script_dir, "profiles", "active")
	p = raw.strip().strip('"').strip("'")
	p = os.path.expandvars(os.path.expanduser(p))
	return p  # accept absolute or relative; user will typically provide absolute (e.g., C:\Users\you\ig_profile)

def resolve_profile_and_cookie(config):
	profile_dir = normalize_profile_dir(config.get("PROFILE_DIR"))
	cookie_file = COOKIE_FILE
	return profile_dir, cookie_file

# --- New: Profile dump scan logic ---
PROFILE_POSTS_PATH = os.path.join('your_instagram_activity', 'media', 'posts_1.json')
LIKED_PATH = os.path.join('your_instagram_activity', 'likes', 'liked_posts.json')
SAVED_COLLECTIONS_PATH = os.path.join('your_instagram_activity', 'saved', 'saved_collections.json')
SAVED_POSTS_PATH = os.path.join('your_instagram_activity', 'saved', 'saved_posts.json')
DM_INBOX_PATH = os.path.join('your_instagram_activity', 'messages', 'inbox')

# --- Safety Settings ---
SAFETY_KEYS = [
    'MIN_DELAY_SECONDS',
    'MAX_DELAY_SECONDS', 
    'LONG_BREAK_EVERY',
    'LONG_BREAK_MIN_SECONDS',
    'LONG_BREAK_MAX_SECONDS',
    'HOURLY_POST_CAP',
    'DAILY_POST_CAP',
    'SAFER_MANUAL_LOGIN',
    'AUTO_RETRY_ON_RATE_LIMIT'
]

SAFETY_PRESETS = {
    'super_duper_schizo_safe': {
        'MIN_DELAY_SECONDS': '60',
        'MAX_DELAY_SECONDS': '120',
        'LONG_BREAK_EVERY': '5',
        'LONG_BREAK_MIN_SECONDS': '200',
        'LONG_BREAK_MAX_SECONDS': '400',
        'HOURLY_POST_CAP': '30',
        'DAILY_POST_CAP': '550',
        'SAFER_MANUAL_LOGIN': 'true',
        'AUTO_RETRY_ON_RATE_LIMIT': 'false'
    },
    'super_safe': {
        'MIN_DELAY_SECONDS': '20',
        'MAX_DELAY_SECONDS': '40',
        'LONG_BREAK_EVERY': '15',
        'LONG_BREAK_MIN_SECONDS': '180',
        'LONG_BREAK_MAX_SECONDS': '300',
        'HOURLY_POST_CAP': '80',
        'DAILY_POST_CAP': '600',
        'SAFER_MANUAL_LOGIN': 'true',
        'AUTO_RETRY_ON_RATE_LIMIT': 'false'
    },
    'standard': {
        'MIN_DELAY_SECONDS': '6',
        'MAX_DELAY_SECONDS': '12',
        'LONG_BREAK_EVERY': '30',
        'LONG_BREAK_MIN_SECONDS': '90',
        'LONG_BREAK_MAX_SECONDS': '180',
        'HOURLY_POST_CAP': '200',
        'DAILY_POST_CAP': '1500',
        'SAFER_MANUAL_LOGIN': 'false',
        'AUTO_RETRY_ON_RATE_LIMIT': 'true'
    },
    'risky': {
        'MIN_DELAY_SECONDS': '2',
        'MAX_DELAY_SECONDS': '5',
        'LONG_BREAK_EVERY': '60',
        'LONG_BREAK_MIN_SECONDS': '60',
        'LONG_BREAK_MAX_SECONDS': '120',
        'HOURLY_POST_CAP': '400',
        'DAILY_POST_CAP': '3000',
        'SAFER_MANUAL_LOGIN': 'false',
        'AUTO_RETRY_ON_RATE_LIMIT': 'true'
    },
    'max_risk': {
        'MIN_DELAY_SECONDS': '0',
        'MAX_DELAY_SECONDS': '0',
        'LONG_BREAK_EVERY': '0',
        'LONG_BREAK_MIN_SECONDS': '0',
        'LONG_BREAK_MAX_SECONDS': '0',
        'HOURLY_POST_CAP': '-1',
        'DAILY_POST_CAP': '-1',
        'SAFER_MANUAL_LOGIN': 'false',
        'AUTO_RETRY_ON_RATE_LIMIT': 'true'
    }
}

class SafetyPacer:
    def __init__(self, cfg, seed_ts):
        self.min_delay = int(cfg['MIN_DELAY_SECONDS'])
        self.max_delay = int(cfg['MAX_DELAY_SECONDS'])
        self.every = int(cfg['LONG_BREAK_EVERY'])
        self.long_min = int(cfg['LONG_BREAK_MIN_SECONDS'])
        self.long_max = int(cfg['LONG_BREAK_MAX_SECONDS'])
        self.hour_cap = int(cfg['HOURLY_POST_CAP'])
        self.day_cap = int(cfg['DAILY_POST_CAP'])
        self.hour_q = deque()
        self.day_q = deque()
        now = time.time()
        for t in (seed_ts or []):
            if t >= now - 86400:
                self.hour_q.append(t)
                self.day_q.append(t)
        self.success_count = 0

    def _unlimited(self, cap): 
        return cap < 0

    def _prune(self):
        now = time.time()
        while self.hour_q and self.hour_q[0] <= now - 3600: 
            self.hour_q.popleft()
        while self.day_q and self.day_q[0] <= now - 86400: 
            self.day_q.popleft()

    def wait_caps(self):
        if self._unlimited(self.hour_cap) and self._unlimited(self.day_cap):
            return
        while True:
            self._prune()
            now = time.time()
            ok_hour = self._unlimited(self.hour_cap) or len(self.hour_q) < self.hour_cap
            ok_day = self._unlimited(self.day_cap) or len(self.day_q) < self.day_cap
            if ok_hour and ok_day:
                return
            sleeps = []
            if not self._unlimited(self.hour_cap) and len(self.hour_q) >= self.hour_cap:
                sleeps.append(self.hour_q[0] + 3600 - now)
            if not self._unlimited(self.day_cap) and len(self.day_q) >= self.day_cap:
                sleeps.append(self.day_q[0] + 86400 - now)
            sleep_time = max(1, int(max(sleeps)))
            if sleep_with_cancel(sleep_time):
                return

    def before_download(self):
        self.wait_caps()
        delay = random.uniform(self.min_delay, self.max_delay)
        if self.max_delay > 0:
            if os.name == 'nt':
                # Windows: non-blocking Enter via msvcrt, still honor shutdown
                print(f"[SAFE] Sleeping {int(delay)}s before download... (Press Enter to skip)")
                import msvcrt
                start = time.time()
                while True:
                    remaining = delay - (time.time() - start)
                    if remaining <= 0:
                        break
                    if SHUTDOWN.is_set():
                        return False
                    if msvcrt.kbhit():
                        key = msvcrt.getch()
                        if key in (b'\r', b'\n'):
                            print("[SAFE] Break skipped by user")
                            break
                    SHUTDOWN.wait(0.05)
            else:
                # POSIX branch
                if sys.stdin.isatty():
                    # Interactive TTY: real non-blocking Enter-to-skip
                    if posix_sleep_with_optional_enter(
                        delay, f"[SAFE] Sleeping {int(delay)}s before download..."
                    ):
                        return False  # shutdown
                else:
                    # Non-interactive (e.g., piped/cron): no hint, cancellable sleep
                    print(f"[SAFE] Sleeping {int(delay)}s before download...")
                    if sleep_with_cancel(delay):
                        return False
        return True

    def after_success(self):
        now = time.time()
        if not self._unlimited(self.hour_cap): 
            self.hour_q.append(now)
        if not self._unlimited(self.day_cap):  
            self.day_q.append(now)
        self.success_count += 1
        if self.every > 0 and (self.success_count % self.every == 0):
            long_break = random.uniform(self.long_min, self.long_max)
            if self.long_max > 0:
                if os.name == 'nt':
                    # Windows: non-blocking Enter via msvcrt, still honor shutdown
                    print(f"[SAFE] Long break: {int(long_break)}s (Press Enter to skip)")
                    import msvcrt
                    start = time.time()
                    while True:
                        remaining = long_break - (time.time() - start)
                        if remaining <= 0:
                            break
                        if SHUTDOWN.is_set():
                            return
                        if msvcrt.kbhit():
                            key = msvcrt.getch()
                            if key in (b'\r', b'\n'):
                                print("[SAFE] Long break skipped by user")
                                break
                        SHUTDOWN.wait(0.05)
                else:
                    if sys.stdin.isatty():
                        if posix_sleep_with_optional_enter(
                            long_break, f"[SAFE] Long break: {int(long_break)}s"
                        ):
                            return  # shutdown requested; caller will notice via SHUTDOWN
                    else:
                        print(f"[SAFE] Long break: {int(long_break)}s")
                        if sleep_with_cancel(long_break):
                            return

# Helper to check if a file exists and is non-empty
def file_exists_nonempty(path):
    return os.path.isfile(path) and os.path.getsize(path) > 0

def scan_profile_dump(dump_path):
    result = {'p': False, 'l': False, 's': False, 'd': False}
    posts_json = os.path.join(dump_path, PROFILE_POSTS_PATH)
    if file_exists_nonempty(posts_json):
        result['p'] = True
    liked_json = os.path.join(dump_path, LIKED_PATH)
    if file_exists_nonempty(liked_json):
        result['l'] = True
    saved_collections_json = os.path.join(dump_path, SAVED_COLLECTIONS_PATH)
    saved_posts_json = os.path.join(dump_path, SAVED_POSTS_PATH)
    if file_exists_nonempty(saved_collections_json) or file_exists_nonempty(saved_posts_json):
        result['s'] = True
    inbox_dir = os.path.join(dump_path, DM_INBOX_PATH)
    if os.path.isdir(inbox_dir):
        for root, dirs, files in os.walk(inbox_dir):
            if 'message_1.json' in files:
                msg_path = os.path.join(root, 'message_1.json')
                if file_exists_nonempty(msg_path):
                    result['d'] = True
                    break
    return result

# --- Cookie handling logic ---
def save_cookies_netscape(driver, cookie_file):
    cookies = driver.get_cookies()
    with open(cookie_file, "w") as f:
        f.write("# Netscape HTTP Cookie File\n")
        f.write("# Generated by NCInstagramDL\n\n")
        for cookie in cookies:
            domain = cookie.get('domain', '')
            domain_specified = 'TRUE' if domain.startswith('.') else 'FALSE'
            path = cookie.get('path', '/')
            secure = 'TRUE' if cookie.get('secure', False) else 'FALSE'
            expiry = str(cookie.get('expiry', 0)) if cookie.get('expiry') else '0'
            name = cookie.get('name', '')
            value = cookie.get('value', '')
            f.write(f"{domain}\t{domain_specified}\t{path}\t{secure}\t{expiry}\t{name}\t{value}\n")

def load_cookies_from_netscape(cookie_file):
    cookies = []
    try:
        with open(cookie_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('#') or not line:
                    continue
                parts = line.split('\t')
                if len(parts) >= 7:
                    domain, domain_specified, path, secure, expiry, name, value = parts[:7]
                    if '.instagram.com' in domain or 'instagram.com' in domain:
                        cookies.append({
                            'name': name,
                            'value': value,
                            'domain': domain,
                            'path': path,
                            'secure': secure.lower() == 'true',
                            'expiry': int(expiry) if expiry.isdigit() else None
                        })
        return cookies
    except Exception:
        return []

def are_cookies_valid(cookie_file=COOKIE_FILE):
    if not os.path.exists(cookie_file):
        return False
    try:
        cookies = load_cookies_from_netscape(cookie_file)
        if not cookies:
            return False
        session = requests.Session()
        for cookie in cookies:
            session.cookies.set(cookie['name'], cookie['value'])
        resp = session.get("https://www.instagram.com/accounts/edit/", allow_redirects=False, timeout=10)
        return resp.status_code == 200
    except Exception:
        return False

# --- End cookie logic ---

# --- Manual login with persistent Chrome profile ---
def manual_login_and_export_cookies(profile_dir: str, cookie_file: str) -> bool:
	os.makedirs(profile_dir, exist_ok=True)
	
	print(f"[Manual Login] Using profile directory: {os.path.abspath(profile_dir)}")
	
	options = build_chrome_options(profile_dir, "1280,900")

	driver = None
	try:
		service = Service(ChromeDriverManager().install())
		driver = webdriver.Chrome(service=service, options=options)

		driver.get("https://www.instagram.com/accounts/login/")
		
		# Remove automation banner if present
		try:
			driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
		except:
			pass
		
		print("\n" + "="*60)
		print("[Manual Login] Chrome opened with profile:", profile_dir)
		print("[Manual Login] Log in manually. Complete any 2FA/checkpoints.")
		print("="*60)
		input("\n[Manual Login] ⚠️  When your feed/profile is visible, press ENTER here... ")

		driver.get("https://www.instagram.com/")
		if sleep_with_cancel(2):
			return False

		save_cookies_netscape(driver, cookie_file)
		print(f"[Manual Login] Cookies exported → {cookie_file}")
		return True
	except Exception as e:
		print(f"[Manual Login] Error: {e}")
		return False
	finally:
		try:
			if driver: driver.quit()
		except:
			pass

def automated_login_and_export_cookies(config, profile_dir: str, cookie_file: str) -> bool:
	options = build_chrome_options(profile_dir, "1280,900")
	
	driver = None
	try:
		service = Service(ChromeDriverManager().install())
		driver = webdriver.Chrome(service=service, options=options)
		
		driver.get('https://www.instagram.com/accounts/login/')
		
		# Remove automation banner if present
		try:
			driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
		except:
			pass
		
		if sleep_with_cancel(2):
			return False
		
		username = config.get('USERNAME')
		password = config.get('PASSWORD')
		
		if not username or not password:
			print("[Auto Login] Missing username or password in config")
			return False
		
		username_field = driver.find_element('name', 'username')
		username_field.clear()
		username_field.send_keys(username)
		password_field = driver.find_element('name', 'password')
		password_field.clear()
		password_field.send_keys(password)
		password_field.submit()
		if sleep_with_cancel(4):
			return False
		
		if 'login' not in driver.current_url.lower():
			print('[Auto Login] Login successful, saving cookies!')
			save_cookies_netscape(driver, cookie_file)
			return True
		
		page_source = driver.page_source
		if 'The username you entered doesn\'t belong to an account' in page_source or 'Sorry, your password was incorrect' in page_source:
			print('[Auto Login] Login failed: incorrect username or password.')
			return False
		
		print('[Auto Login] Login failed: still on login page.')
		return False
		
	except Exception as e:
		print(f"[Auto Login] Exception during Selenium login: {e}")
		return False
	finally:
		try:
			if driver: driver.quit()
		except:
			pass

def ensure_valid_cookies(config) -> bool:
	profile_dir, cookie_file = resolve_profile_and_cookie(config)
	SAFER_MANUAL_LOGIN = parse_bool(config.get("SAFER_MANUAL_LOGIN"), True)
	
	if are_cookies_valid(cookie_file):
		print("Valid cookies found. Skipping login.")
		return True

	if SAFER_MANUAL_LOGIN:
		print("SAFER_MANUAL_LOGIN is ON → manual login.")
		while True:
			if manual_login_and_export_cookies(profile_dir, cookie_file) and are_cookies_valid(cookie_file):
				return True
			print("[WARN] Cookies still invalid. Finish login in Chrome and press ENTER again.")
	else:
		print("SAFER_MANUAL_LOGIN is OFF → attempting automated login (max 3 tries).")
		for attempt in range(1, 4):
			print(f"[Auto Login] Attempt {attempt}/3...")
			if automated_login_and_export_cookies(config, profile_dir, cookie_file) and are_cookies_valid(cookie_file):
				return True
		print("[Auto Login] Failed after 3 attempts. Falling back to manual.")
		while True:
			if manual_login_and_export_cookies(profile_dir, cookie_file) and are_cookies_valid(cookie_file):
				return True
			print("[WARN] Cookies still invalid. Finish login in Chrome and press ENTER again.")

# --- Config I/O helpers ---
def load_config_with_structure():
    """
    Load config.txt into an ordered structure retaining original lines/comments.
    
    Returns:
        dict: Config with 'lines' (list of original lines) and 'values' (dict of key-value pairs)
    """
    config = {'lines': [], 'values': {}}
    
    if not os.path.exists(CONFIG_FILE):
        return config
    
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            config['lines'].append(line.rstrip('\n'))
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, value = line.split('=', 1)
                config['values'][key.strip()] = value.strip()
    
    return config

def get_safety_config():
    """
    Get safety configuration values, normalizing legacy 0→-1 for caps.
    
    Returns:
        dict: Safety configuration with normalized values
    """
    config = load_config_with_structure()
    safety_config = {}
    
    for key in SAFETY_KEYS:
        if key == 'SAFER_MANUAL_LOGIN':
            value = config['values'].get(key, 'true')  # Default to true for SAFER_MANUAL_LOGIN
        elif key == 'AUTO_RETRY_ON_RATE_LIMIT':
            value = config['values'].get(key, 'true')  # Default to true for AUTO_RETRY_ON_RATE_LIMIT
        else:
            value = config['values'].get(key, '0')
        # Normalize legacy 0 caps to -1
        if key in ['HOURLY_POST_CAP', 'DAILY_POST_CAP'] and value == '0':
            value = '-1'
        safety_config[key] = value
    
    return safety_config

def save_config(new_values):
    """
    Save config with new values.
    
    Args:
        new_values: dict of key-value pairs to update
    """
    # Load current config
    config = load_config_with_structure()
    
    # Separate safety keys from comments
    safety_updates = {}
    preset_comment = None
    
    for key, value in new_values.items():
        if key.startswith('#'):
            preset_comment = f"{key}={value}"
        else:
            safety_updates[key] = value
            config['values'][key] = value
    
    # Write updated config
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        for line in config['lines']:
            # Skip existing SAFETY_PRESET_APPLIED lines
            if line.strip().startswith('# SAFETY_PRESET_APPLIED='):
                continue
            if line.strip() and '=' in line:
                key = line.split('=', 1)[0].strip()
                if key in safety_updates:
                    # Update existing line
                    f.write(f"{key}={safety_updates[key]}\n")
                    continue
            f.write(line + '\n')
        
        # Add any missing safety keys
        for key, value in safety_updates.items():
            if not any(key in line for line in config['lines']):
                f.write(f"{key}={value}\n")
        
        # Add preset comment if provided
        if preset_comment:
            f.write(f"{preset_comment}\n")

def _prompt_int(prompt_text, default=None, allow_blank=True, allow_minus1=False):
	while True:
		s = input(prompt_text).strip()
		if s.lower() == 'b':
			return 'BACK'
		if s == '' and allow_blank:
			return default
		try:
			val = int(s)
			if allow_minus1 and val == -1:
				return val
			return val
		except ValueError:
			print("Enter an integer (or 'b' to go back).")

def edit_delay_pair(cfg):
	print("\nPer-download delay (seconds). Press Enter to keep current. 'b' = back.")
	min_cur = int(cfg.get('MIN_DELAY_SECONDS', 0))
	max_cur = int(cfg.get('MAX_DELAY_SECONDS', 0))

	min_new = _prompt_int(f"  Min [{min_cur}]: ", default=min_cur)
	if min_new == 'BACK': return False
	max_new = _prompt_int(f"  Max [{max_cur}]: ", default=max_cur)
	if max_new == 'BACK': return False

	if min_new < 0 or max_new < 0:
		print("  Min/Max must be ≥ 0.")
		return edit_delay_pair(cfg)
	if min_new > max_new:
		resp = input(f"  Min {min_new} > Max {max_new}. Set Max to {min_new}? (y/N): ").strip().lower()
		if resp == 'y':
			max_new = min_new
		else:
			return edit_delay_pair(cfg)

	cfg['MIN_DELAY_SECONDS'] = str(min_new)
	cfg['MAX_DELAY_SECONDS'] = str(max_new)
	return True

def edit_long_break_pair(cfg):
	print("\nLong break settings. 'Every' of 0 disables long breaks. 'b' = back.")
	every_cur = int(cfg.get('LONG_BREAK_EVERY', 0))
	every_new = _prompt_int(f"  Every N downloads [{every_cur}]: ", default=every_cur)
	if every_new == 'BACK': return False

	min_cur = int(cfg.get('LONG_BREAK_MIN_SECONDS', 0))
	max_cur = int(cfg.get('LONG_BREAK_MAX_SECONDS', 0))

	if every_new == 0:
		cfg['LONG_BREAK_EVERY'] = '0'
		cfg['LONG_BREAK_MIN_SECONDS'] = '0'
		cfg['LONG_BREAK_MAX_SECONDS'] = '0'
		return True

	min_new = _prompt_int(f"  Break min seconds [{min_cur}]: ", default=min_cur)
	if min_new == 'BACK': return False
	max_new = _prompt_int(f"  Break max seconds [{max_cur}]: ", default=max_cur)
	if max_new == 'BACK': return False

	if min_new <= 0 or max_new <= 0:
		print("  Break min/max must be > 0 when enabled.")
		return edit_long_break_pair(cfg)
	if min_new > max_new:
		resp = input(f"  Break min {min_new} > max {max_new}. Set max to {min_new}? (y/N): ").strip().lower()
		if resp == 'y':
			max_new = min_new
		else:
			return edit_long_break_pair(cfg)

	cfg['LONG_BREAK_EVERY'] = str(every_new)
	cfg['LONG_BREAK_MIN_SECONDS'] = str(min_new)
	cfg['LONG_BREAK_MAX_SECONDS'] = str(max_new)
	return True

def edit_hourly_cap(cfg):
	print("\nHourly cap (-1 = No cap). 'b' = back.")
	hr_cur = int(cfg.get('HOURLY_POST_CAP', -1))
	hr_new = _prompt_int(f"  Hourly cap [{hr_cur}] (-1=no cap): ", default=hr_cur, allow_minus1=True)
	if hr_new == 'BACK': return False
	if hr_new == 0 or hr_new < -1:
		print("  Cap must be -1 or ≥ 1 (0 invalid).")
		return edit_hourly_cap(cfg)
	cfg['HOURLY_POST_CAP'] = str(hr_new)
	return True

def edit_daily_cap(cfg):
	print("\nDaily cap (-1 = No cap). 'b' = back.")
	dy_cur = int(cfg.get('DAILY_POST_CAP', -1))
	dy_new = _prompt_int(f"  Daily cap [{dy_cur}] (-1=no cap): ", default=dy_cur, allow_minus1=True)
	if dy_new == 'BACK': return False
	if dy_new == 0 or dy_new < -1:
		print("  Cap must be -1 or ≥ 1 (0 invalid).")
		return edit_daily_cap(cfg)
	cfg['DAILY_POST_CAP'] = str(dy_new)
	return True

def edit_safer_manual_login(cfg):
	print("\nSAFER_MANUAL_LOGIN setting. 'b' = back.")
	current = cfg.get('SAFER_MANUAL_LOGIN', 'true').lower()
	print(f"  Current: {current}")
	print("  Options: true/false, on/off, 1/0, yes/no, y/n")
	print("  Press Enter to keep current value")
	
	while True:
		choice = input("  New value (true/false): ").strip().lower()
		if choice == 'b':
			return False
		if choice == '':
			return True  # Keep current value
		if choice in ('true', '1', 'yes', 'y', 'on'):
			cfg['SAFER_MANUAL_LOGIN'] = 'true'
			return True
		elif choice in ('false', '0', 'no', 'n', 'off'):
			cfg['SAFER_MANUAL_LOGIN'] = 'false'
			return True
		else:
			print("  Invalid choice. Please enter true/false, on/off, 1/0, yes/no, or y/n.")

def edit_auto_retry_on_rate_limit(cfg):
	print("\nAUTO_RETRY_ON_RATE_LIMIT setting. 'b' = back.")
	current = cfg.get('AUTO_RETRY_ON_RATE_LIMIT', 'true').lower()
	print(f"  Current: {current}")
	print("  Options: true/false, on/off, 1/0, yes/no, y/n")
	print("  Press Enter to keep current value")
	
	while True:
		choice = input("  New value (true/false): ").strip().lower()
		if choice == 'b':
			return False
		if choice == '':
			return True  # Keep current value
		if choice in ('true', '1', 'yes', 'y', 'on'):
			cfg['AUTO_RETRY_ON_RATE_LIMIT'] = 'true'
			return True
		elif choice in ('false', '0', 'no', 'n', 'off'):
			cfg['AUTO_RETRY_ON_RATE_LIMIT'] = 'false'
			return True
		else:
			print("  Invalid choice. Please enter true/false, on/off, 1/0, yes/no, or y/n.")

def edit_profile_dir(cfg):
	print("\nPROFILE_DIR setting (Chrome profile directory). 'b' = back.")
	current = cfg.get('PROFILE_DIR', '')
	print(f"  Current: {current if current else '(default: <repo>/profiles/active)'}")
	print("  Enter full path (e.g., C:\\Users\\you\\ig_profile) or leave blank for default")
	
	choice = input("  New value: ").strip()
	if choice == 'b':
		return False
	
	if choice:
		# Normalize the path
		normalized = normalize_profile_dir(choice)
		cfg['PROFILE_DIR'] = choice  # Keep original user input
		
		# Try to create directory
		try:
			os.makedirs(normalized, exist_ok=True)
			print(f"  Profile directory: {normalized}")
		except Exception as e:
			print(f"  Warning: Could not create directory {normalized}: {e}")
	else:
		cfg['PROFILE_DIR'] = ''
		print("  Using default profile directory")
	
	return True

def validate_safety_config(config):
    """
    Validate safety configuration values.
    
    Args:
        config: dict of safety configuration values
        
    Returns:
        tuple: (is_valid, error_message)
    """
    try:
        min_delay = int(config['MIN_DELAY_SECONDS'])
        max_delay = int(config['MAX_DELAY_SECONDS'])
        every = int(config['LONG_BREAK_EVERY'])
        long_min = int(config['LONG_BREAK_MIN_SECONDS'])
        long_max = int(config['LONG_BREAK_MAX_SECONDS'])
        hour_cap = int(config['HOURLY_POST_CAP'])
        day_cap = int(config['DAILY_POST_CAP'])
        
        # Validation rules
        if min_delay < 0 or max_delay < 0:
            return False, "Delays cannot be negative"
        if min_delay > max_delay:
            return False, "MIN_DELAY_SECONDS cannot be greater than MAX_DELAY_SECONDS"
        if every < 0:
            return False, "LONG_BREAK_EVERY cannot be negative"
        if every > 0 and (long_min <= 0 or long_max <= 0):
            return False, "Long break delays must be positive when enabled"
        if every > 0 and long_min > long_max:
            return False, "LONG_BREAK_MIN_SECONDS cannot be greater than LONG_BREAK_MAX_SECONDS"
        if hour_cap == 0 or day_cap == 0:
            return False, "Caps cannot be 0 (use -1 for no cap)"
        if hour_cap < -1 or day_cap < -1:
            return False, "Caps cannot be less than -1"
        
        return True, ""
        
    except ValueError as e:
        return False, f"Invalid numeric value: {e}"



def sanitize_filename(filename):
    """
    Sanitize filename by removing/replacing invalid characters.
    
    Args:
        filename: Raw filename string
        
    Returns:
        str: Sanitized filename safe for filesystem
    """
    # Remove or replace invalid characters
    filename = unicodedata.normalize('NFKD', filename)
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    filename = re.sub(r'\s+', ' ', filename)  # Replace multiple spaces with single
    filename = filename.strip()
    return filename

def slug_from_send_text(s: str, max_len: int = 40) -> str:
	if not s: return ""
	s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
	# keep letters/numbers/space/_/-
	s = re.sub(r"[^A-Za-z0-9 _-]+", "", s)
	# collapse whitespace -> underscores
	s = re.sub(r"\s+", "_", s).strip("_")
	return s[:max_len] if len(s) >= 3 else ""

def to_file_uri(path: str) -> str:
	p = os.path.abspath(path)
	if os.name == 'nt':
		# file:///C:/...
		return 'file:///' + p.replace('\\', '/')
	return 'file://' + p

def is_abs_pathish(s: str) -> bool:
	s = s.strip()
	if not s:
		return False
	# Windows C:\ or \\server\share
	if os.name == 'nt':
		return bool(re.match(r'^[a-zA-Z]:\\', s)) or s.startswith('\\\\')
	# POSIX
	return s.startswith('/')

def generate_filename(post_data, max_length=200):
    """
    Generate structured filename for downloaded posts.
    
    Args:
        post_data: Dictionary containing post information
        max_length: Maximum filename length (default 200)
        
    Returns:
        str: Generated filename template
    """
    parts = []
    
    # Always include shortcode
    shortcode = post_data.get('shortcode', '')
    if shortcode:
        parts.append(shortcode)
    
    # Add send message suffix if enabled and available
    if post_data.get('append_send_for_this_run') and post_data.get('send_text') and post_data.get('source') in ('dm','dm_profile'):
        slug = slug_from_send_text(post_data['send_text'])
        if slug:
            parts.append(slug)
    
    # Add original owner if present
    original_owner = post_data.get('original_owner', '')
    if original_owner:
        parts.append(f"by_{original_owner}")
    
    # Add caption if present and there's room
    caption = post_data.get('caption', '')
    if caption:
        current_len = sum(len(p) + 1 for p in parts)
        remaining = max_length - current_len - 10
        if remaining > 10:
            parts.append(clean_text_for_filename(caption, max_len=remaining))
    
    # Join parts with underscores
    filename = "_".join(parts)
    
    # Ensure we don't exceed max length
    if len(filename) > max_length:
        filename = filename[:max_length-3] + "..."
    
    # Add extension placeholder
    filename += ".%(ext)s"
    
    return filename

def extract_shortcode_from_url(url):
    """
    Extract Instagram shortcode from URL.
    
    Args:
        url: Instagram post URL
        
    Returns:
        str: Shortcode or None if not found
    """
    # Handle various Instagram URL formats
    patterns = [
        r'instagram\.com/p/([A-Za-z0-9_-]+)',
        r'instagram\.com/reel/([A-Za-z0-9_-]+)',
        r'instagram\.com/tv/([A-Za-z0-9_-]+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    return None

def ensure_unique_dir(base_dir: str, name: str) -> str:
	"""
	Create a unique subfolder of base_dir using a sanitized name.
	If the folder exists, append -2, -3, ... until unique.
	"""
	safe = sanitize_filename(name) or "thread"
	target = os.path.join(base_dir, safe)
	if not os.path.exists(target):
		os.makedirs(target, exist_ok=True)
		return target
	# Resolve collisions conservatively
	i = 2
	while True:
		cand = os.path.join(base_dir, f"{safe}-{i}")
		if not os.path.exists(cand):
			os.makedirs(cand, exist_ok=True)
			return cand
		i += 1

def ensure_thread_dir(base_dir: str, name: str) -> str:
	"""
	Create a stable subfolder of base_dir using a sanitized name.
	Reuses existing folder if it exists (no suffixes).
	"""
	safe = sanitize_filename(name) or "thread"
	path = os.path.join(base_dir, safe)
	os.makedirs(path, exist_ok=True)
	return path

def _shortcode_from_share_link(url: str) -> str | None:
	if not url: return None
	try:
		u = urlparse(url)
		host = (u.netloc or "").lower()
		if "instagram.com" not in host:
			return None
		parts = [p for p in (u.path or "").split("/") if p]
		if not parts: 
			return None
		# Accept only structured share types; no raw-link fallback
		if parts[0] in ("reel", "p", "tv") and len(parts) > 1:
			return parts[1]
		return None
	except Exception:
		return None

def extract_dm_posts_and_profiles(dm_json_path, thread_name=None):
    """
    Extract posts and profiles from DM JSON file.
    
    Args:
        dm_json_path: Path to message_1.json file
        thread_name: Name of the DM thread/conversation
        
    Returns:
        tuple: (posts_list, profiles_list, send_text_hits)
    """
    posts = []
    profiles = []
    send_text_hits = 0
    
    try:
        with open(dm_json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Navigate to messages array
        messages = data.get('messages', [])
        
        # First pass: collect all shares and their timestamps/senders
        shares = []
        for i, message in enumerate(messages):
            if 'share' in message:
                share = message['share']
                link = share.get('link', '')
                
                if not link:
                    continue
                
                # Check if it's a profile share
                if '/_u/' in link:
                    # Profile share
                    username_match = re.search(r'/_u/([^/]+)', link)
                    if username_match:
                        profile_data = {
                            'username': username_match.group(1),
                            'profile_name': share.get('share_text', ''),
                            'timestamp': message.get('timestamp_ms', 0)
                        }
                        profiles.append(profile_data)
                else:
                    # Post share
                    shortcode = extract_shortcode_from_url(link)
                    if shortcode:
                        share_data = {
                            'index': i,
                            'shortcode': shortcode,
                            'url': link,
                            'original_owner': share.get('original_owner', ''),
                            'share_text': share.get('share_text', ''),
                            'timestamp_ms': message.get('timestamp_ms', 0),
                            'sender': message.get('sender_name', ''),
                            'source': 'dm',
                            'dm_thread': thread_name
                        }
                        shares.append(share_data)
        
        # Second pass: for each share, look for send messages
        for share_data in shares:
            share_ts = share_data['timestamp_ms']
            share_sender = share_data['sender']
            share_index = share_data['index']
            
            # Look for send message: next human text message by same sender within 1s
            send_text = None
            for j in range(share_index + 1, len(messages)):
                msg = messages[j]
                msg_ts = msg.get('timestamp_ms', 0)
                msg_sender = msg.get('sender_name', '')
                
                # Check if this is a human text message (not a system/reaction line)
                if 'content' in msg and msg_sender == share_sender:
                    time_diff = msg_ts - share_ts
                    if 0 < time_diff < 1000:  # Strictly under 1s
                        send_text = msg['content']
                        send_text_hits += 1
                        break
                    elif time_diff >= 1000:  # Too late, stop looking
                        break
            
            # Create post data
            raw = (share_data['share_text'] or '').strip() or None
            post_data = {
                'shortcode': share_data['shortcode'],
                'url': share_data['url'],
                'original_owner': share_data['original_owner'],
                'caption_raw': raw,
                'caption': normalize_caption_text(raw) if raw else None,
                'timestamp_ms': share_data['timestamp_ms'],
                'source': 'dm',
                'dm_thread': thread_name,
                'send_text': send_text  # Add send_text field
            }
            posts.append(post_data)
    
    except Exception as e:
        print(f"Error parsing DM JSON {dm_json_path}: {e}")
    
    return posts, profiles, send_text_hits

def download_post(conn, post_data, download_dir, pacer=None):
	"""
	Download a single Instagram post using yt-dlp with fallback to gallery-dl.
	
	Args:
		conn: Database connection
		post_data: Dictionary containing post information
		download_dir: Directory to save the download
		pacer: SafetyPacer instance for rate limiting
		
	Returns:
		bool: True if download successful, False otherwise
	"""
	shortcode = post_data.get('shortcode')
	url = post_data.get('url')
	
	if not shortcode or not url:
		print(f"Missing shortcode or URL for post")
		return False
	
	# Check if already downloaded
	if is_downloaded(conn, shortcode):
		print(f"[SKIPPED] {shortcode} already recorded")
		SESSION_TRACKER.record_download_skip()
		return True
	
	# Record download attempt
	SESSION_TRACKER.record_download_attempt()
	
	# Safety pacing before download; honor cancel
	if pacer:
		if not pacer.before_download() or SHUTDOWN.is_set():
			return False
    
	# Generate filename
	filename_template = generate_filename(post_data)
	output_path = os.path.join(download_dir, filename_template)
	
	print(f"Downloading {shortcode}...")
	
	# Try yt-dlp first
	try:
		cmd = [
			'yt-dlp',
			'--cookies', COOKIE_FILE,
			'--output', output_path,
			'--no-check-certificate',
			'--ignore-errors',
			'--print', 'after_move:filepath',   # NEW: print final saved file path
			url
		]
		
		result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
		
		# Check for rate limit errors
		if result.returncode != 0:
			reason = classify_block_reason(result.stderr)
			if reason == "rate_limit":
				raise RateLimitError(result.stderr or "rate limited")
			elif reason == "checkpoint":
				raise CheckpointError(result.stderr or "checkpoint")
			elif reason == "login_required":
				raise LoginRequiredError(result.stderr or "login required")
		
		if result.returncode == 0:
			# extract final path from stdout (last non-empty line)
			std_lines = [l for l in result.stdout.splitlines() if l.strip()]
			saved_path = std_lines[-1].strip() if std_lines else None
			if saved_path and not os.path.isabs(saved_path):
				saved_path = os.path.abspath(os.path.join(download_dir, saved_path))
			status = record_download(conn, post_data, saved_path)   # pass path
			if status == "inserted":
				fname = os.path.basename(saved_path) if saved_path else f"{shortcode}"
				print(f"Successfully downloaded and recorded {fname}")
				if saved_path:
					print(f"[LINK]  {to_file_uri(saved_path)}")
				if pacer:
					pacer.after_success()
				SESSION_TRACKER.record_download_success()
			elif status == "duplicate":
				print(f"[DUPLICATE] {shortcode} already in database")
				SESSION_TRACKER.record_download_skip()
			else:
				print(f"[ERROR] {shortcode} → database error")
				SESSION_TRACKER.record_error(f"Database error for {shortcode}")
			return True
		else:
			print(f"yt-dlp failed for {shortcode}: {result.stderr}")
			
	except subprocess.TimeoutExpired:
		print(f"yt-dlp timeout for {shortcode}")
	except Exception as e:
		print(f"yt-dlp error for {shortcode}: {e}")
    
	# Fallback to gallery-dl
	try:
		# Create gallery-dl specific filename template with proper extension handling
		# gallery-dl uses {extension} instead of %(ext)s
		base_filename = filename_template.replace('.%(ext)s', '')
		
		# Flat file pattern for gallery-dl; matches yt-dlp base, adds {num} for carousels
		gallery_filename = f"{base_filename}_{{num}}.{{extension}}"

		cmd = [
			'gallery-dl',
			'--cookies', COOKIE_FILE,
			'--directory', download_dir,
			'--filename', gallery_filename,
			'--exec', 'echo {filepath}',
			url
		]
		
		result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
		
		# Check for rate limit errors
		if result.returncode != 0:
			reason = classify_block_reason(result.stderr)
			if reason == "rate_limit":
				raise RateLimitError(result.stderr or "rate limited")
			elif reason == "checkpoint":
				raise CheckpointError(result.stderr or "checkpoint")
			elif reason == "login_required":
				raise LoginRequiredError(result.stderr or "login required")
		
		if result.returncode == 0:
			# collect all absolute-looking paths gallery-dl printed
			candidates = [l.strip() for l in result.stdout.splitlines() if is_abs_pathish(l)]
			saved_path = candidates[-1] if candidates else None
			
			# If exactly one created file, drop the trailing "_1" in its stem
			if len(candidates) == 1:
				stem, ext = os.path.splitext(os.path.basename(saved_path))
				# expected stem pattern: f"{base_filename}_1"
				# build the expected stem to be safe
				expected_prefix = os.path.basename(base_filename)
				if stem == f"{expected_prefix}_1":
					dst = os.path.join(download_dir, f"{expected_prefix}{ext}")
					if not os.path.exists(dst):
						try:
							os.rename(saved_path, dst)
							saved_path = dst
						except Exception as _e:
							# If rename fails, keep original path; continue
							pass
			
			if saved_path and not os.path.isabs(saved_path):
				saved_path = os.path.abspath(os.path.join(download_dir, saved_path))
			status = record_download(conn, post_data, saved_path)   # pass path
			if status == "inserted":
				fname = os.path.basename(saved_path) if saved_path else f"{shortcode}"
				print(f"Successfully downloaded and recorded {fname}")
				if saved_path:
					print(f"[LINK]  {to_file_uri(saved_path)}")
				if pacer:
					pacer.after_success()
			elif status == "duplicate":
				print(f"[DUPLICATE] {shortcode} already in database")
			else:
				print(f"[ERROR] {shortcode} → database error")
			return True
		else:
			print(f"gallery-dl failed for {shortcode}: {result.stderr}")
			
	except subprocess.TimeoutExpired:
		print(f"gallery-dl timeout for {shortcode}")
	except Exception as e:
		print(f"gallery-dl error for {shortcode}: {e}")
    
	# Record failure
	error_msg = f"Both yt-dlp and gallery-dl failed to download {shortcode}"
	status = record_failure(conn, post_data, error_msg)
	if status == "inserted":
		print(f"[ERROR] {shortcode} → {error_msg}")
		SESSION_TRACKER.record_download_failure()
		SESSION_TRACKER.record_error(f"Download failed: {shortcode} - {error_msg}")
	elif status == "duplicate":
		print(f"[DUPLICATE] {shortcode} failure already recorded")
	else:
		print(f"[ERROR] {shortcode} → database error recording failure")
		SESSION_TRACKER.record_error(f"Database error recording failure for {shortcode}")
	
	# Log total failure to file for debugging
	try:
		from datetime import datetime
		timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
		failure_log_entry = f"[{timestamp}] {shortcode} - {error_msg} - URL: {url}\n"
		
		with open("total_failures.log", "a", encoding="utf-8") as f:
			f.write(failure_log_entry)
		
		print(f"[DEBUG] Total failure logged to total_failures.log")
	except Exception as e:
		print(f"[DEBUG] Failed to log to file: {e}")
	
	return False



def extract_urls_from_current_page(driver, username):
    """Extract URLs and captions from the current page state"""
    urls = set()
    post_data = {}
    
    # Try multiple selectors for post links
    selectors = [
        "a[href*='/p/']",
        "a[href*='/reel/']",
        "article a[href*='/p/']",
        "article a[href*='/reel/']",
        "div[role='button'] a[href*='/p/']",
        "div[role='button'] a[href*='/reel/']"
    ]
    
    for selector in selectors:
        try:
            post_links = driver.find_elements(By.CSS_SELECTOR, selector)
            
            for link in post_links:
                href = link.get_attribute('href')
                if href and ('/p/' in href or '/reel/' in href):
                    urls.add(href)
                    
                    # Try to get caption from the post
                    try:
                        # Look for caption in nearby elements
                        caption_element = link.find_element(By.XPATH, ".//ancestor::article//div[contains(@class, 'caption') or contains(@class, 'text')]")
                        caption = caption_element.text.strip()
                        if caption:
                            post_data[href] = caption
                    except:
                        # No caption found, that's okay
                        pass
        except Exception as e:
            # Silently continue if selector fails
            pass
    
    return urls, post_data

def get_profile_post_urls(username):
    """Get all post URLs from a profile using Selenium"""
    print(f"[PROFILE] Getting post URLs for @{username} using Selenium")
    
    try:
        # Dictionary to store URLs and their captions
        post_data = {}
        
        # Get profile directory from config
        config = read_config()
        profile_dir, _ = resolve_profile_and_cookie(config)
        
        # Set up Chrome options
        options = build_chrome_options(profile_dir, "1280,900")
        
        # Create driver
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        
        try:
            # Navigate to profile
            profile_url = f"https://www.instagram.com/{username}/"
            print(f"[PROFILE] Loading profile page: {profile_url}")
            
            driver.get(profile_url)
            
            # Wait for page to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Wait for posts to load - look for the post grid
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "article"))
                )
                print(f"[PROFILE] Found post grid for @{username}")
            except:
                print(f"[PROFILE] Post grid not found, trying to scroll for @{username}")
            
            # Scroll down to load more posts
            if sleep_with_cancel(3):
                return [], {}
            
            # Scroll down to load all posts and collect URLs during scrolling
            previous_height = driver.execute_script("return document.body.scrollHeight")
            scroll_attempts = 0
            max_scrolls = 20  # Limit to prevent infinite scrolling
            all_urls = set()  # Use set to avoid duplicates
            all_post_data = {}
            
            while scroll_attempts < max_scrolls:
                # Collect URLs before scrolling
                current_urls, current_post_data = extract_urls_from_current_page(driver, username)
                all_urls.update(current_urls)
                all_post_data.update(current_post_data)
                
                print(f"[PROFILE] After scroll {scroll_attempts}: Found {len(all_urls)} total URLs for @{username}")
                
                # Scroll down
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                if sleep_with_cancel(2):
                    return [], {}
                
                # Check if new content loaded
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == previous_height:
                    # No new content loaded, we've reached the bottom
                    print(f"[PROFILE] Reached bottom after {scroll_attempts} scrolls for @{username}")
                    break
                
                previous_height = new_height
                scroll_attempts += 1
                print(f"[PROFILE] Scrolled {scroll_attempts}/{max_scrolls} for @{username}")
            
            # Final collection after scrolling is done
            final_urls, final_post_data = extract_urls_from_current_page(driver, username)
            all_urls.update(final_urls)
            all_post_data.update(final_post_data)
            
            # Wait a bit more for posts to load
            if sleep_with_cancel(3):
                return [], {}
            
            # Get page source
            content = driver.page_source
            
            # Convert sets back to lists for return
            urls = list(all_urls)
            print(f"[PROFILE] Total unique URLs found: {len(urls)} for @{username}")
            
            if urls:
                print(f"[PROFILE] Found {len(urls)} posts via Selenium for @{username}")
                return urls, all_post_data
            
            # Fallback: try to find shortcodes in the page source
            print(f"[PROFILE] Trying fallback method for @{username}")
            
            # Look for shortcodes in the HTML
            shortcode_pattern = r'"shortcode":"([A-Za-z0-9_-]{11})"'
            shortcodes = re.findall(shortcode_pattern, content)
            
            if shortcodes:
                urls = []
                for shortcode in shortcodes:
                    urls.append(f"https://www.instagram.com/p/{shortcode}/")
                
                print(f"[PROFILE] Found {len(urls)} posts via HTML parsing for @{username}")
                return urls, post_data
            
            # Another fallback: look for post URLs directly
            post_url_pattern = r'https://www\.instagram\.com/p/([A-Za-z0-9_-]{11})/'
            post_urls = re.findall(post_url_pattern, content)
            
            if post_urls:
                urls = []
                for shortcode in post_urls:
                    urls.append(f"https://www.instagram.com/p/{shortcode}/")
                
                print(f"[PROFILE] Found {len(urls)} posts via URL pattern for @{username}")
                return urls, post_data
            
            print(f"[FAILED] No posts found for @{username}")
            return [], {}
            
        finally:
            driver.quit()
        
    except Exception as e:
        print(f"[ERROR] Error using Selenium for @{username}: {e}")
        return [], {}

def download_profile_posts(conn, username, download_dir, source='dm_profile', pacer=None, thread_name=None, safety_config=None, append_send_for_this_run=False):
    """
    Download all posts from a user's profile using Selenium scraping.
    
    Args:
        conn: Database connection
        username: Instagram username
        download_dir: Directory to save downloads
        source: Source identifier for database
        pacer: SafetyPacer instance for rate limiting
        thread_name: DM thread name for database tracking
        safety_config: Safety configuration
        append_send_for_this_run: Whether to append send messages to filenames
        
    Returns:
        bool: True if any posts were downloaded successfully
    """
    print(f"[PROFILE] Downloading all posts from @{username}")
    
    try:
        # First, get all post URLs from the profile using Selenium
        result = get_profile_post_urls(username)
        
        if not result or len(result) != 2:
            print(f"[FAILED] No post URLs found for @{username}")
            return False
        
        post_urls, post_data = result
        
        if not post_urls:
            print(f"[FAILED] No post URLs found for @{username}")
            return False
        
        # Only create profile-specific folder if we actually have posts to download
        profile_dir = os.path.join(download_dir, sanitize_filename(username))
        os.makedirs(profile_dir, exist_ok=True)
        
        print(f"[PROFILE] Found {len(post_urls)} posts to download for @{username}")
        
        # Download each post individually using our existing download_post method
        successful_downloads = 0
        skipped_count = 0
        
        for i, post_url in enumerate(post_urls, 1):
            if SHUTDOWN.is_set():
                break
            print(f"[PROFILE] Downloading post {i}/{len(post_urls)}: {post_url}")
            
            # Extract shortcode from URL
            shortcode = extract_shortcode_from_url(post_url)
            if not shortcode:
                print(f"[FAILED] Could not extract shortcode from {post_url}")
                continue
            
            # Check if already downloaded
            if is_downloaded(conn, shortcode):
                print(f"[SKIP] {shortcode} already downloaded for @{username}")
                skipped_count += 1
                continue
            
            # Get caption for this post if available
            post_caption = post_data.get(post_url, "")
            
            # Create post data structure
            raw = (post_caption or '').strip() or None
            post_data_dict = {
                'shortcode': shortcode,
                'url': post_url,
                'original_owner': username,
                'caption_raw': raw,
                'caption': normalize_caption_text(raw) if raw else None,
                'timestamp_ms': int(time.time() * 1000),
                'source': source,
                'dm_thread': thread_name,
                'append_send_for_this_run': append_send_for_this_run
            }
            
            # Use our existing download method with exception handling
            retry_count = 0
            while True:
                try:
                    ok = download_post(conn, post_data_dict, profile_dir, pacer)
                    # success or non-block failure -> break to next item
                    if ok:
                        successful_downloads += 1
                        print(f"[SUCCESS] Downloaded post {i}/{len(post_urls)} for @{username}")
                    else:
                        print(f"[FAILED] Post {i}/{len(post_urls)} for @{username}")
                    break
                except RateLimitError as e:
                    SESSION_TRACKER.record_rate_limit()
                    print(f"\n[BLOCK] Rate limited.")
                    print("[Advice] Waiting ~30–60 minutes is safest before retrying to avoid repeated blocks.")
                    auto_retry = parse_bool(safety_config.get('AUTO_RETRY_ON_RATE_LIMIT'), True) if safety_config else True
                    if auto_retry:
                        base_delay = RATE_LIMIT_SCHEDULE[min(retry_count, len(RATE_LIMIT_SCHEDULE)-1)]
                        delay = get_jittered_delay(base_delay)
                        print(f"[BLOCK] Auto-retrying this item in {delay}s (base: {base_delay}s + jitter)...")
                        if sleep_with_cancel(delay):
                            return False  # Shutdown requested
                        retry_count += 1
                        continue
                    # interactive mode
                    if retry_count > 0:  # delayed retry mode
                        base_delay = RATE_LIMIT_SCHEDULE[min(retry_count, len(RATE_LIMIT_SCHEDULE)-1)]
                        delay = get_jittered_delay(base_delay)
                        print(f"[BLOCK] Delayed retry mode: sleeping {delay}s (base: {base_delay}s + jitter) before retry...")
                        if sleep_with_cancel(delay):
                            return False  # Shutdown requested
                        retry_count += 1
                        continue
                    resp = input("[Enter]=retry now  |  D=delayed exponential retry  |  S=skip this item  |  Q=quit run > ").strip().lower()
                    if resp == "q":
                        return False
                    if resp == "s":
                        record_failure(conn, post_data_dict, "Skipped after rate limit")
                        SESSION_TRACKER.record_download_skip()
                        break
                    if resp == "d":
                        retry_count = 0  # start delayed retry mode at first step
                        continue  # loop will sleep next time
                    # default: immediate retry
                    continue
                except CheckpointError as e:
                    print(f"\n[BLOCK] Checkpoint/challenge.")
                    print("[Advice] Complete MANUAL LOGIN with the same persistent profile (or wait/switch), then retry.")
                    print("[Advice] After clearing the challenge, waiting ~30–60 minutes before resuming is safest.")
                    resp = input("[Enter]=retry  |  M=manual login now  |  S=skip  |  Q=quit > ").strip().lower()
                    if resp == "q":
                        return False
                    if resp == "s":
                        record_failure(conn, post_data_dict, "Skipped during checkpoint")
                        SESSION_TRACKER.record_download_skip()
                        break
                    if resp == "m":
                        # Get current profile and cookie file
                        config = read_config()
                        profile_dir, cookie_file = resolve_profile_and_cookie(config)
                        if manual_login_and_export_cookies(profile_dir, cookie_file):
                            print("[BLOCK] Manual login completed, retrying...")
                        else:
                            print("[BLOCK] Manual login failed, retrying anyway...")
                        # else retry immediately
                except LoginRequiredError as e:
                    print(f"\n[BLOCK] Login required (cookies/session invalid).")
                    print("[Advice] Revalidate cookies via MANUAL LOGIN, then retry.")
                    resp = input("[M]=manual login now  |  R=retry with current cookies  |  S=skip  |  Q=quit > ").strip().lower()
                    if resp == "q":
                        return False
                    if resp == "s":
                        record_failure(conn, post_data_dict, "Skipped after login-required")
                        SESSION_TRACKER.record_download_skip()
                        break
                    if resp == "m":
                        # Get current profile and cookie file
                        config = read_config()
                        profile_dir, cookie_file = resolve_profile_and_cookie(config)
                        if manual_login_and_export_cookies(profile_dir, cookie_file):
                            print("[BLOCK] Manual login completed, retrying...")
                        else:
                            print("[BLOCK] Manual login failed, retrying anyway...")
                    # else retry immediately
            

        
        if successful_downloads > 0:
            print(f"[SUCCESS] Downloaded {successful_downloads}/{len(post_urls)} posts from @{username} (skipped {skipped_count})")
            return True
        else:
            print(f"[FAILED] No posts downloaded from @{username}")
            return False
            
    except Exception as e:
        print(f"[ERROR] Profile @{username} - {e}")
        return False

def process_dm_download(conn, selected_path, pacer=None, safety_config=None):
    """
    Process DM downloads from a selected profile dump.
    
    Args:
        conn: Database connection
        selected_path: Path to the selected profile dump
        pacer: SafetyPacer instance for rate limiting
        
    Returns:
        bool: True if processing completed successfully
    """
    print("Processing DM downloads...")
    
    # Find message_1.json files in the inbox
    inbox_dir = os.path.join(selected_path, DM_INBOX_PATH)
    if not os.path.exists(inbox_dir):
        print(f"DM inbox directory not found: {inbox_dir}")
        return False
    
    message_files = []
    for root, dirs, files in os.walk(inbox_dir):
        if 'message_1.json' in files:
            message_files.append(os.path.join(root, 'message_1.json'))
    
    if not message_files:
        print("No message files found in DM inbox")
        return False
    
    print(f"Found {len(message_files)} DM conversations")
    
    # Display available DM conversations
    print("\nAvailable DM conversations:")
    for i, msg_file in enumerate(message_files, 1):
        thread_name = os.path.basename(os.path.dirname(msg_file))
        print(f"{i}. {thread_name}")
    print("a) Download all conversations")
    print("b) Back to options menu")
    print("q) Quit")
    
    # Get user selection
    while True:
        choice = input("\nSelect which conversation(s) to download (number, 'a' for all, 'b' for back, or 'q' to quit): ").strip().lower()
        
        if choice == 'q':
            print("Quitting.")
            return False
        elif choice == 'b':
            print("Returning to options menu.")
            return None  # Special return value to indicate "back"
        elif choice == 'a':
            selected_files = message_files
            break
        elif choice.isdigit():
            num = int(choice)
            if 1 <= num <= len(message_files):
                selected_files = [message_files[num - 1]]
                break
            else:
                print(f"Invalid choice. Please enter a number between 1 and {len(message_files)}, 'a', 'b', or 'q'.")
        else:
            print(f"Invalid choice. Please enter a number between 1 and {len(message_files)}, 'a', 'b', or 'q'.")
    
    # Get download directory from config
    config = read_config()
    download_base_dir = config.get('DOWNLOAD_DIRECTORY', os.path.join(os.path.dirname(__file__), 'downloads'))
    
    # Parse send message append config
    ASK_FOR_SEND_MESSAGE_APPEND = parse_bool(config.get("ASK_FOR_SEND_MESSAGE_APPEND"), False)
    
    # Create download directory structure
    dm_download_dir = os.path.join(download_base_dir, "dms")
    os.makedirs(dm_download_dir, exist_ok=True)
    
    print(f"Downloads will be saved to: {dm_download_dir}")
    
    total_posts = 0
    total_profiles = 0
    
    for msg_file in selected_files:
        thread_name = os.path.basename(os.path.dirname(msg_file))
        print(f"\nProcessing {thread_name}...")
        
        thread_dir = ensure_thread_dir(dm_download_dir, thread_name)
        print(f"[DM] Saving this conversation to: {thread_dir}")
        
        # Gather all message parts for this DM thread
        thread_root = os.path.dirname(msg_file)  # msg_file is the selected message_*.json
        part_files = sorted(glob(os.path.join(thread_root, "message_*.json")))

        all_msgs = []
        for pf in part_files:
            try:
                with open(pf, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    all_msgs.extend(data.get("messages", []))
            except Exception as e:
                print(f"[DM] Skipping {pf}: {e}")

        # Sort by ascending timestamp for reliable <1s pairing across parts
        all_msgs.sort(key=lambda m: m.get("timestamp_ms", 0))

        seen_shortcodes = set()
        posts = []
        send_text_hits = 0

        for i, m in enumerate(all_msgs):
            share = m.get("share") or {}
            link = share.get("link")
            if not link:
                continue  # no raw-link fallback; only structured shares

            shortcode = _shortcode_from_share_link(link)
            if not shortcode or shortcode in seen_shortcodes:
                continue
            seen_shortcodes.add(shortcode)

            ts = m.get("timestamp_ms")
            sender = (m.get("sender_name") or "").strip()
            orig_owner = (share.get("original_content_owner") or "").strip() or None
            caption_raw = (share.get("share_text") or "").strip() or None

            # Pair a send message if next is same sender and within <1s
            send_text = None
            if i + 1 < len(all_msgs):
                nxt = all_msgs[i + 1]
                if (nxt.get("sender_name") or "").strip() == sender:
                    nts = nxt.get("timestamp_ms")
                    if isinstance(ts, int) and isinstance(nts, int) and 0 <= (nts - ts) <= 1000:
                        if isinstance(nxt.get("content"), str) and nxt["content"].strip():
                            send_text = nxt["content"].strip()
                            send_text_hits += 1

            post = {
                'shortcode': shortcode,
                'url': link,
                'description': None,
                'original_owner': orig_owner,
                'caption_raw': caption_raw,
                'caption': normalize_caption_text(caption_raw) if caption_raw else None,
                'source': 'dm',
                'username': orig_owner,
                'timestamp_ms': ts,
                'dm_thread': thread_name,
            }
            if send_text:
                post['send_text'] = send_text

            posts.append(post)

        print(f"Found {len(posts)} shared posts from {len(part_files)} message parts")
        
        # Check for send message append option
        append_send_for_this_run = False
        if ASK_FOR_SEND_MESSAGE_APPEND and send_text_hits > 0:
            print(f"Detected {send_text_hits} send messages (<1s after shares) in this conversation.")
            choice = input("Append them to filenames for this run? [y/N]: ").strip().lower()
            append_send_for_this_run = (choice == 'y')
        
        # Download the collected posts for this thread into the per-thread folder
        for i, post in enumerate(posts, 1):
            if SHUTDOWN.is_set():
                break
            print(f"Downloading post {i}/{len(posts)}: {post['shortcode']}")
            
            # Add send message flag to post data
            post['append_send_for_this_run'] = append_send_for_this_run
            
            # Use our existing download method with exception handling
            retry_count = 0
            while True:
                try:
                    ok = download_post(conn, post, thread_dir, pacer)
                    # success or non-block failure -> break to next item
                    if ok:
                        total_posts += 1
                    break
                except RateLimitError as e:
                    SESSION_TRACKER.record_rate_limit()
                    print(f"\n[BLOCK] Rate limited.")
                    print("[Advice] Waiting ~30–60 minutes is safest before retrying to avoid repeated blocks.")
                    auto_retry = parse_bool(safety_config.get('AUTO_RETRY_ON_RATE_LIMIT'), True) if safety_config else True
                    if auto_retry:
                        base_delay = RATE_LIMIT_SCHEDULE[min(retry_count, len(RATE_LIMIT_SCHEDULE)-1)]
                        delay = get_jittered_delay(base_delay)
                        print(f"[BLOCK] Auto-retrying this item in {delay}s (base: {base_delay}s + jitter)...")
                        if sleep_with_cancel(delay):
                            return False  # Shutdown requested
                        retry_count += 1
                        continue
                    # interactive mode
                    if retry_count > 0:  # delayed retry mode
                        base_delay = RATE_LIMIT_SCHEDULE[min(retry_count, len(RATE_LIMIT_SCHEDULE)-1)]
                        delay = get_jittered_delay(base_delay)
                        print(f"[BLOCK] Delayed retry mode: sleeping {delay}s (base: {base_delay}s + jitter) before retry...")
                        if sleep_with_cancel(delay):
                            return False  # Shutdown requested
                        retry_count += 1
                        continue
                    resp = input("[Enter]=retry now  |  D=delayed exponential retry  |  S=skip this item  |  Q=quit run > ").strip().lower()
                    if resp == "q":
                        return False
                    if resp == "s":
                        record_failure(conn, post, "Skipped after rate limit")
                        SESSION_TRACKER.record_download_skip()
                        break
                    if resp == "d":
                        retry_count = 0  # start delayed retry mode at first step
                        continue  # loop will sleep next time
                    # default: immediate retry
                    continue
                except CheckpointError as e:
                    print(f"\n[BLOCK] Checkpoint/challenge.")
                    print("[Advice] Complete MANUAL LOGIN with the same persistent profile (or wait/switch), then retry.")
                    print("[Advice] After clearing the challenge, waiting ~30–60 minutes before resuming is safest.")
                    resp = input("[Enter]=retry  |  M=manual login now  |  S=skip  |  Q=quit > ").strip().lower()
                    if resp == "q":
                        return False
                    if resp == "s":
                        record_failure(conn, post, "Skipped during checkpoint")
                        SESSION_TRACKER.record_download_skip()
                        break
                    if resp == "m":
                        # Get current profile and cookie file
                        config = read_config()
                        profile_dir, cookie_file = resolve_profile_and_cookie(config)
                        if manual_login_and_export_cookies(profile_dir, cookie_file):
                            print("[BLOCK] Manual login completed, retrying...")
                        else:
                            print("[BLOCK] Manual login failed, retrying anyway...")
                        # else retry immediately
                except LoginRequiredError as e:
                    print(f"\n[BLOCK] Login required (cookies/session invalid).")
                    print("[Advice] Revalidate cookies via MANUAL LOGIN, then retry.")
                    resp = input("[M]=manual login now  |  R=retry with current cookies  |  S=skip  |  Q=quit > ").strip().lower()
                    if resp == "q":
                        return False
                    if resp == "s":
                        record_failure(conn, post, "Skipped after login-required")
                        SESSION_TRACKER.record_download_skip()
                        break
                    if resp == "m":
                        # Get current profile and cookie file
                        config = read_config()
                        profile_dir, cookie_file = resolve_profile_and_cookie(config)
                        if manual_login_and_export_cookies(profile_dir, cookie_file):
                            print("[BLOCK] Manual login completed, retrying...")
                        else:
                            print("[BLOCK] Manual login failed, retrying anyway...")
                    # else retry immediately
    
    if not SHUTDOWN.is_set():
        print(f"\nDM download complete!")
        print(f"Total posts downloaded: {total_posts}")
        print(f"Total profiles processed: {total_profiles}")
    
    return True

def read_config():
    config = {}
    if not os.path.exists(CONFIG_FILE):
        print(f"Config file not found: {CONFIG_FILE}")
        exit(1)
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, value = line.split('=', 1)
                config[key.strip()] = value.strip()
    return config



def get_profile_dumps_dir():
    config = read_config()
    if 'PROFILE_DUMP_DIRECTORY' not in config:
        print("PROFILE_DUMP_DIRECTORY not set in config.txt")
        exit(1)
    return config['PROFILE_DUMP_DIRECTORY']

def get_profile_dumps():
    PROFILE_DUMPS_DIR = get_profile_dumps_dir()
    if not os.path.exists(PROFILE_DUMPS_DIR):
        print(f"Profile dumps directory not found: {PROFILE_DUMPS_DIR}")
        return []
    entries = []
    for name in os.listdir(PROFILE_DUMPS_DIR):
        full_path = os.path.join(PROFILE_DUMPS_DIR, name)
        if os.path.isdir(full_path):
            m = re.search(r'(\d{4}-\d{2}-\d{2})', name)
            date_str = m.group(1) if m else ''
            entries.append((name, full_path, date_str))
    entries.sort(key=lambda x: x[2], reverse=True)
    return [(name, path) for name, path, _ in entries]

def print_page(dumps, dump_availability, page):
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_items = dumps[start:end]
    print("Select which profile dump to download from ({} available):".format(len(dumps)))
    print("Legend: [p] Profile Posts | [l] Liked | [s] Saved | [d] DMs\n")
    for idx, (name, _) in enumerate(page_items, start + 1):
        avail = dump_availability.get(name, {'p': False, 'l': False, 's': False, 'd': False})
        flags = ''.join([c if avail[c] else ' ' for c in 'plsd'])
        print(f"{idx:2d}. {name:<35} [{flags}]")
    if len(dumps) > PAGE_SIZE:
        if end < len(dumps):
            print("n) Next page")
        if page > 0:
            print("p) Previous page")
    print("c) Config Menu")
    print("q) Quit")

def print_options_menu(avail):
    options = []
    if avail['p']:
        options.append("Profile Posts Download")
    if avail['l']:
        options.append("Liked Posts Download")
    if avail['s']:
        options.append("Saved Posts Download")
    if avail['d']:
        options.append("DM Download")
    print("\nAvailable download options:")
    for i, opt in enumerate(options, 1):
        print(f"{i}. {opt}")
    print("b) Back to main menu")
    print("q) Quit")
    return options

def view_safety_settings():
    """Display current safety settings"""
    config = get_safety_config()
    
    print("\n=== Safety Settings ===")
    print(f"MIN_DELAY_SECONDS: {config['MIN_DELAY_SECONDS']}")
    print(f"MAX_DELAY_SECONDS: {config['MAX_DELAY_SECONDS']}")
    print(f"LONG_BREAK_EVERY: {config['LONG_BREAK_EVERY']}")
    print(f"LONG_BREAK_MIN_SECONDS: {config['LONG_BREAK_MIN_SECONDS']}")
    print(f"LONG_BREAK_MAX_SECONDS: {config['LONG_BREAK_MAX_SECONDS']}")
    
    hour_cap = config['HOURLY_POST_CAP']
    day_cap = config['DAILY_POST_CAP']
    print(f"HOURLY_POST_CAP: {hour_cap if hour_cap != '-1' else 'No cap'}")
    print(f"DAILY_POST_CAP: {day_cap if day_cap != '-1' else 'No cap'}")
    
    safer_login = config.get('SAFER_MANUAL_LOGIN', 'true')
    print(f"SAFER_MANUAL_LOGIN: {safer_login}")
    
    auto_retry = config.get('AUTO_RETRY_ON_RATE_LIMIT', 'true')
    print(f"AUTO_RETRY_ON_RATE_LIMIT: {auto_retry}")

def apply_safety_preset():
    """Apply a safety preset"""
    print("\n=== Safety Presets ===")
    print("1. super_duper_schizo_safe - Ultra conservative, maximum human-like behavior")
    print("2. super_safe - Very conservative, minimal risk")
    print("3. standard - Balanced safety and speed")
    print("4. risky - Faster downloads, higher risk")
    print("5. max_risk - No delays, maximum speed")
    
    while True:
        choice = input("\nSelect preset (1-5) or b to back: ").strip().lower()
        if choice == 'b':
            return
        
        if choice in ['1', '2', '3', '4', '5']:
            preset_names = ['super_duper_schizo_safe', 'super_safe', 'standard', 'risky', 'max_risk']
            preset_name = preset_names[int(choice) - 1]
            preset_values = dict(SAFETY_PRESETS[preset_name])
            
            # Add preset comment
            preset_values['# SAFETY_PRESET_APPLIED'] = preset_name
            
            try:
                save_config(preset_values)
                print(f"Applied {preset_name} preset successfully!")
                return
            except Exception as e:
                print(f"Error applying preset: {e}")
                return
        
        print("Invalid choice. Please enter 1-5 or b.")

def edit_safety_values():
    """Edit individual safety values with staged flow"""
    cfg_current = get_safety_config()
    pending = dict(cfg_current)
    original_config = dict(cfg_current)
    
    print("\n=== Edit Safety Values ===")
    print("Press 'b' at any prompt to go back and cancel all changes.")
    
    # Stage 1: Per-download delay (pair)
    if not edit_delay_pair(pending):
        return
    
    # Stage 2: Long break (pair)
    if not edit_long_break_pair(pending):
        return
    
    # Stage 3: Hourly cap (independent)
    if not edit_hourly_cap(pending):
        return
    
    # Stage 4: Daily cap (independent)
    if not edit_daily_cap(pending):
        return
    
    # Stage 5: Manual login setting (independent)
    if not edit_safer_manual_login(pending):
        return
    
    # Stage 6: Auto retry rate limit setting (independent)
    if not edit_auto_retry_on_rate_limit(pending):
        return
    
    # Show summary of changes
    changes = {}
    for key in SAFETY_KEYS:
        if pending[key] != original_config[key]:
            changes[key] = (original_config[key], pending[key])
    
    if changes:
        print("\n=== Changes Summary ===")
        for key, (old_val, new_val) in changes.items():
            print(f"{key}: {old_val} → {new_val}")
        
        save_choice = input("\nSave changes? (y/n): ").strip().lower()
        if save_choice == 'y':
            try:
                # Add custom preset comment
                pending['# SAFETY_PRESET_APPLIED'] = 'custom'
                save_config(pending)
                print("Changes saved successfully!")
            except Exception as e:
                print(f"Error saving changes: {e}")
        else:
            print("Changes discarded.")
    else:
        print("No changes made.")

def settings_menu():
    """Main settings menu"""
    while True:
        print("\n=== Settings ===")
        print("1. View safety settings")
        print("2. Apply safety preset")
        print("3. Edit individual values")
        print("b) Back to main menu")
        print("q) Quit")
        
        choice = input("\nEnter your choice: ").strip().lower()
        
        if choice == '1':
            view_safety_settings()
        elif choice == '2':
            apply_safety_preset()
        elif choice == '3':
            edit_safety_values()
        elif choice == 'b':
            break
        elif choice == 'q':
            print("Quitting.")
            exit(0)
        else:
            print("Invalid choice. Please try again.")

def main():
    config = read_config()
    
    # Install signal handlers early
    install_signal_handlers()
    
    # --- Pre-flight ffmpeg check ---
    if not check_ffmpeg_availability():
        print_ffmpeg_warning()
        # Continue anyway, but user is warned
    
    # Initialize SQLite database
    db_path = os.path.join(os.path.dirname(__file__), 'downloaded_posts.db')
    conn = init_db(db_path)
    
    try:
        # --- New cookie gate with manual/automated login flow ---
        if not ensure_valid_cookies(config):
            print("[FATAL] Could not obtain valid cookies.")
            return
        
        # Initialize SafetyPacer
        safety_config = get_safety_config()
        recent_timestamps = get_recent_download_timestamps(conn, time.time() - 86400)
        pacer = SafetyPacer(safety_config, recent_timestamps)
        
        # Proceed to main script
        dumps = get_profile_dumps()
        if not dumps:
            print("No profile dumps found.")
            return
        
        dump_availability = {}
        for name, path in dumps:
            dump_availability[name] = scan_profile_dump(path)
        
        page = 0
        while True:
            print_page(dumps, dump_availability, page)
            if len(dumps) > PAGE_SIZE:
                prompt_msg = "Enter your choice (number, n, p, c, q): "
                invalid_msg = f"Invalid option. Please enter a number between 1 and {len(dumps)}, 'n', 'p', 'c', or 'q'."
            else:
                prompt_msg = "Enter your choice (number, c, q): "
                invalid_msg = f"Invalid option. Please enter a number between 1 and {len(dumps)}, 'c', or 'q'."
            
            choice = input(prompt_msg).strip().lower()
            if choice.isdigit():
                num = int(choice)
                if 1 <= num <= len(dumps):
                    selected = dumps[num - 1]
                    selected_name, selected_path = selected
                    avail = dump_availability[selected_name]
                    options = print_options_menu(avail)
                    
                    if not options:
                        print("No download options available for this profile dump.")
                        input("Press Enter to return to main menu...")
                        continue
                    
                    while True:
                        opt_choice = input("Enter your choice (number, b, or q): ").strip().lower()
                        if opt_choice == 'q':
                            print("Quitting.")
                            return
                        elif opt_choice == 'b':
                            break
                        elif opt_choice.isdigit():
                            opt_num = int(opt_choice)
                            if 1 <= opt_num <= len(options):
                                selected_option = options[opt_num-1]
                                print(f"You selected: {selected_option}")
                                
                                # Handle different download options
                                if "DM Download" in selected_option:
                                    result = process_dm_download(conn, selected_path, pacer, safety_config)
                                    if result is True:
                                        # Print download statistics only if completed
                                        print("\nDownload Statistics:")
                                        stats = get_download_stats(conn)
                                        for key, value in stats.items():
                                            print(f"  {key}: {value}")
                                        input("Press Enter to return to main menu...")
                                        break
                                    elif result is False:
                                        # User quit the program
                                        return
                                    elif result is None:
                                        # User chose back, re-display options menu
                                        print_options_menu(avail)
                                        continue
                                elif "Profile Posts Download" in selected_option:
                                    print("Profile posts download not yet implemented")
                                    input("Press Enter to return to main menu...")
                                    break
                                elif "Liked Posts Download" in selected_option:
                                    print("Liked posts download not yet implemented")
                                    input("Press Enter to return to main menu...")
                                    break
                                elif "Saved Posts Download" in selected_option:
                                    print("Saved posts download not yet implemented")
                                    input("Press Enter to return to main menu...")
                                    break
                            else:
                                print(f"Invalid option. Please enter a number between 1 and {len(options)}, 'b', or 'q'.")
                        else:
                            print(f"Invalid option. Please enter a number between 1 and {len(options)}, 'b', or 'q'.")
                    # If we broke out of the inner loop, continue to the outer loop (back to main menu)
                    continue
                else:
                    print(invalid_msg)
                    input("Press Enter to continue...")
            elif len(dumps) > PAGE_SIZE and choice == 'n' and (page + 1) * PAGE_SIZE < len(dumps):
                page += 1
            elif len(dumps) > PAGE_SIZE and choice == 'p' and page > 0:
                page -= 1
            elif choice == 'c':
                settings_menu()
                # Refresh safety config after settings change
                safety_config = get_safety_config()
                recent_timestamps = get_recent_download_timestamps(conn, time.time() - 86400)
                pacer = SafetyPacer(safety_config, recent_timestamps)
            elif choice == 'q':
                print("Quitting.")
                break
            else:
                print(invalid_msg)
                input("Press Enter to continue...")
    
    finally:
        # Print session summary on exit
        print(SESSION_TRACKER.get_session_summary())
        
        # Clean exit - close database connection
        close_db(conn)

if __name__ == "__main__":
    main() 