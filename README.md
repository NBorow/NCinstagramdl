# NCInstagramDL

A tool for managing and downloading Instagram data dumps, including future support for direct message (DM) downloads.

## Features
- Browse and select Instagram profile data dumps
- (Planned) Download and process Instagram direct messages

## Installation

1. **Clone the repository:**
   ```sh
   git clone <your-repo-url>
   cd NCinstagramdl
   ```

2. **Install dependencies:**
   This project requires Python 3.7+ and the following packages:
   - `requests`
   - `beautifulsoup4`
   - `lxml`
   - `tqdm`
   - `pytz`
   - `dateparser`
   - `emoji`
   - `chardet`
   - `python-dateutil`
   - `selenium`
   - `webdriver-manager`
   
   Install them with:
   ```sh
   pip install requests beautifulsoup4 lxml tqdm pytz dateparser emoji chardet python-dateutil selenium webdriver-manager
   ```

## Configuration

Create a `config.txt` file in the project root with the following contents (edit the paths as needed):

```
PROFILE_DUMP_DIRECTORY=C:\example_directory
DOWNLOAD_DIRECTORY=C:\example_directory2
# Optional: Instagram login credentials for DM download/verification
# USERNAME=your_instagram_username
# PASSWORD=your_instagram_password
```

### Login Credentials Behavior
- If both `USERNAME` and `PASSWORD` are present in `config.txt`, the script will attempt to log in with them.
- If only `USERNAME` is present, you will be prompted for the password at runtime.
- If only `PASSWORD` is present (but not `USERNAME`), the script will ignore the password and prompt for both username and password.
- If neither is present, you will be prompted for both.
- The script will verify the credentials by attempting a headless login to Instagram before proceeding. If the login fails, you will be prompted to re-enter your credentials.
- **Do not use quotes** around the username or password in `config.txt`.

## Downloading Your Instagram Data

1. Visit the [Instagram Data Download page](https://accountscenter.instagram.com/info_and_permissions/dyi/).
2. Request your account information. Wait for Instagram to email you a download link.
3. Download the ZIP file from the email.
4. Unzip the contents into your profile dump directory (as set in `config.txt`).

## Running the Script

From the project directory, run:

```sh
python ncinstagramdl.py
```

Follow the on-screen prompts to log in (if needed), select and process your Instagram data dump, and choose available download options.

---

*More features and integration with DM downloading coming soon!*
