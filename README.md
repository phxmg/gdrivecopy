# Google Drive Folder Copy Tool

A script to copy Google Drive folders with their complete structure and files from a shared folder to your own drive. This script is designed to work with folders that are shared with you through Google Drive.

## Features

- Copies entire folder structures while preserving hierarchy
- Handles large files without creating ZIP archives
- Works with shared folders even with limited permissions
- Provides detailed progress information during copying
- Supports dry-run mode to simulate copying without making changes

## Requirements

- Python 3.6 or above
- Google account with access to source and destination folders

## Setup

1. Install the required Python packages:

```bash
pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib
```

2. Create a Google Cloud project and enable the Drive API:
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project
   - Enable the Google Drive API
   - Configure OAuth consent screen (select "Internal" if you have a G Suite account)
   - Create OAuth client ID credentials (select "Desktop application")
   - Download the credentials JSON file and save it as `credentials.json` in the same directory as the script

## Usage

### Basic Usage

To copy the "Gateway Footage" folder to your Drive:

```bash
python gdrive_direct.py
```

### Dry Run Mode

To simulate the copy process without making any changes:

```bash
python gdrive_direct.py --dry-run
```

### Custom Source and Destination

To specify custom source and destination folder IDs:

```bash
python gdrive_direct.py --source SOURCE_ID --dest DESTINATION_ID
```

## Command-line Options

- `--dry-run`: Simulate the copy process without making any changes
- `--source SOURCE_ID`: Source folder ID (defaults to Gateway Footage folder)
- `--dest DEST_ID`: Destination folder ID (defaults to "Gen Gateway" folder)
- `--max-retries N`: Maximum number of retry attempts for rate limited operations (default: 8)
- `--initial-delay N`: Initial delay in seconds before first retry (default: 10)
- `--backoff-factor N`: Factor by which the delay increases with each retry (default: 2)
- `--operation-delay N`: Optional delay in seconds between file operations (default: 0, no delay)

## Rate Limiting and API Quotas

The Google Drive API has usage limits that can cause errors when copying large numbers of files. This script includes several features to handle rate limiting:

1. **Exponential backoff**: When rate limit errors occur, the script waits progressively longer between retries
2. **Operation delays**: The script adds a delay between file operations to avoid hitting limits
3. **Quota checking**: Use the included `check_quota.py` script to verify your API quota status

For severe rate limiting issues, you can add an operation delay:

```bash
python gdrive_direct.py --operation-delay 3 --initial-delay 20
```

## Additional Scripts

- **gdrive_direct.py**: Main script for copying folders with rate limit handling
- **check_quota.py**: Tool to check Google Drive API quota status
- **gdrive_batch.py**: Advanced script for batch processing with longer delays

## How It Works

1. The script first connects to your Google Drive account
2. It verifies access to the source folder
3. It lists all contents of the source folder
4. You select a destination folder in your Drive
5. The script copies all folders and files while preserving the structure
6. For files that can't be copied directly, it uses a fallback download/upload method

## Notes

- The script preserves the folder structure from the source folder
- Large files are handled properly without needing to create ZIP archives
- Progress is shown during copying
- On first run, it will open a browser window for authentication
- Handles macOS resource fork files (._* files) along with regular files