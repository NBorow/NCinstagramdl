import os
import re

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