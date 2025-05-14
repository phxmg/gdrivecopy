# Interactive Google Drive Folder Copy

This interactive tool helps you copy shared folders to your own Google Drive. It features a friendly user interface, guiding you through each step of the process.

## Features

- **Focused on Folders**: Shows only shared folders, not individual shared files
- **Full Structure Copying**: Copies all files and subfolders within shared folders
- **Shared Drive Support**: Works with both regular shared folders and Shared Drives
- **Colorful, User-Friendly Interface**: Easy to navigate with clear instructions
- **Real-time Progress**: Shows detailed progress during copying operations
- **Preserves Structure**: Maintains the exact folder hierarchy from source to destination

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

Simply run the script and follow the interactive prompts:

```bash
python gdrive_interactive.py
```

The script will guide you through 5 simple steps:
1. Connect to Google Drive and show you a list of folders shared with you
2. Let you select a destination folder in your own Drive
3. Preview the contents of the source folder
4. Preview the destination folder
5. Copy the entire folder with all contents (files and subfolders)

## Example Session

```
====================================================================================
                       Google Drive Folder Copy Tool
====================================================================================

This tool helps you copy shared folders to your own Google Drive.
It preserves the folder structure and works with large files.
✓ Successfully connected to Google Drive

Step 1/5: Finding folders shared with you
Searching for folders shared with you...

Found 3 shared folders:
1. 📁 Gateway Footage [Shared by: John Smith] [ID: 1YdkvUODE7r7KYHe7FtjxFbb3Uy-ecqA7]
2. 📁 Project Assets [Shared by: Jane Doe] [ID: 2XaZcVbA8Pq7LmNs5TrW3Ye9Uh4FgD2K]
3. 📁 Financial Reports [Shared Drive] [ID: 3BnMqRtY6WpL9ZaX8SvU7Hd5Gf4Jp2E1]

Enter the number of the shared folder you want to copy (or 'q' to quit): 1
```

## Notes

- The script will only show folders (not individual files) shared with you
- When copying from a shared folder, it will copy ALL files and subfolders within it
- Very large files might take some time to copy
- The script does not delete any files from the source folder
- On first run, it will open a browser window for authentication