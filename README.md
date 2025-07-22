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
   
   Install them with:
   ```sh
   pip install requests beautifulsoup4 lxml tqdm pytz dateparser emoji chardet python-dateutil
   ```

## Configuration

Create a `config.txt` file in the project root with the following contents (edit the paths as needed):

```
PROFILE_DUMP_DIRECTORY=C:\example_directory
DOWNLOAD_DIRECTORY=C:\example_directory2
```

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

Follow the on-screen prompts to select and process your Instagram data dump.

---

*More features and integration with DM downloading coming soon!*
