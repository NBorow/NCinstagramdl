import os
import re
import getpass
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'config.txt')
PAGE_SIZE = 10

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

def verify_login_selenium(username, password):
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.get('https://www.instagram.com/accounts/login/')
        import time
        time.sleep(2)  # Wait for page to load
        # Find and fill username
        username_field = driver.find_element('name', 'username')
        username_field.clear()
        username_field.send_keys(username)
        # Find and fill password
        password_field = driver.find_element('name', 'password')
        password_field.clear()
        password_field.send_keys(password)
        # Submit form
        password_field.submit()
        time.sleep(4)  # Wait for login to process
        # Check for login success by looking for the presence of the profile icon or absence of login form
        if 'login' not in driver.current_url.lower():
            driver.quit()
            return True
        # Alternatively, check for error message
        page_source = driver.page_source
        if 'The username you entered doesn\'t belong to an account' in page_source or 'Sorry, your password was incorrect' in page_source:
            driver.quit()
            return False
        # Fallback: if still on login page, treat as failure
        driver.quit()
        return False
    except Exception as e:
        print(f"[Login Error] {e}")
        return False

def prompt_for_credentials(config):
    username = config.get('USERNAME')
    password = config.get('PASSWORD')
    # If password is present but username is not, ignore password and prompt for both
    if not username and password:
        password = None
    while True:
        if not username:
            username = input('Enter Instagram username: ').strip()
        if not password:
            password = getpass.getpass('Enter Instagram password: ')
        print('Verifying Instagram login...')
        if verify_login_selenium(username, password):
            print('Login successful!')
            return username, password
        else:
            print('Login failed. Please try again.')
            username = input('Enter Instagram username: ').strip()
            password = getpass.getpass('Enter Instagram password: ')

def get_profile_dumps_dir():
    config = read_config()
    if 'PROFILE_DUMP_DIRECTORY' not in config:
        print("PROFILE_DUMP_DIRECTORY not set in config.txt")
        exit(1)
    return config['PROFILE_DUMP_DIRECTORY']

def get_profile_dumps():
    """Return a list of (folder_name, full_path) for all profile dumps, sorted by date in name (descending)."""
    PROFILE_DUMPS_DIR = get_profile_dumps_dir()
    if not os.path.exists(PROFILE_DUMPS_DIR):
        print(f"Profile dumps directory not found: {PROFILE_DUMPS_DIR}")
        return []
    entries = []
    for name in os.listdir(PROFILE_DUMPS_DIR):
        full_path = os.path.join(PROFILE_DUMPS_DIR, name)
        if os.path.isdir(full_path):
            # Try to extract date from folder name
            m = re.search(r'(\d{4}-\d{2}-\d{2})', name)
            date_str = m.group(1) if m else ''
            entries.append((name, full_path, date_str))
    # Sort by date string descending (most recent first)
    entries.sort(key=lambda x: x[2], reverse=True)
    return [(name, path) for name, path, _ in entries]

def print_page(dumps, page):
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_items = dumps[start:end]
    print(f"Select which profile dump to download from ({len(dumps)} available):")
    for idx, (name, _) in enumerate(page_items, start + 1):
        print(f"{idx}. {name}")
    if end < len(dumps):
        print("n) Next page")
    if page > 0:
        print("p) Previous page")
    print("q) Quit")

def main():
    config = read_config()
    # Login verification step
    username, password = prompt_for_credentials(config)
    # Proceed to main script
    dumps = get_profile_dumps()
    if not dumps:
        print("No profile dumps found.")
        return
    page = 0
    while True:
        os.system('cls' if os.name == 'nt' else 'clear')
        print_page(dumps, page)
        choice = input("Enter your choice (number, n, p, q): ").strip().lower()
        if choice.isdigit():
            num = int(choice)
            if 1 <= num <= len(dumps):
                selected = dumps[num - 1]
                print(f"You selected: {selected[0]}")
                break
            else:
                print(f"Invalid option. Please enter a number between 1 and {len(dumps)}, 'n', 'p', or 'q'.")
                input("Press Enter to continue...")
        elif choice == 'n' and (page + 1) * PAGE_SIZE < len(dumps):
            page += 1
        elif choice == 'p' and page > 0:
            page -= 1
        elif choice == 'q':
            print("Quitting.")
            break
        else:
            print(f"Invalid option. Please enter a number between 1 and {len(dumps)}, 'n', 'p', or 'q'.")
            input("Press Enter to continue...")

if __name__ == "__main__":
    main() 