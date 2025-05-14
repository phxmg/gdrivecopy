#!/usr/bin/env python3

import os
import pickle
import sys
import argparse
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.readonly']

# Default folder IDs from Google Drive URLs
DEFAULT_SOURCE_FOLDER_ID = '1YdkvUODE7r7KYHe7FtjxFbb3Uy-ecqA7'  # "Gateway Footage" 
DEFAULT_DESTINATION_FOLDER_ID = '1IXsLFhcDRUhe_c-Kbm_GeLqI6z7UrB5I'  # "Gen Gateway"

# Parse command line arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description='Copy files/folders from one Google Drive folder to another.')
    parser.add_argument('-s', '--source', help='Source folder ID (shared folder to copy from)')
    parser.add_argument('-d', '--destination', help='Destination folder ID (your folder to copy to)')
    parser.add_argument('-y', '--yes', action='store_true', help='Automatically proceed without confirmation')
    parser.add_argument('-l', '--list-only', action='store_true', help='Only list folders, don\'t copy')
    return parser.parse_args()

# Debugging function to verify folder existence
def verify_folder_access(service, folder_id, folder_name):
    try:
        folder = service.files().get(fileId=folder_id).execute()
        print(f"✓ Successfully accessed {folder_name}: {folder['name']} (ID: {folder_id})")
        return True
    except Exception as e:
        print(f"✗ Error accessing {folder_name} with ID {folder_id}: {str(e)}")
        return False

def get_credentials():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return creds

def list_files_and_folders(service, folder_id):
    """List files and folders in the specified folder."""
    results = []
    page_token = None
    
    try:
        # Try to use standard list method first
        while True:
            try:
                response = service.files().list(
                    q=f"'{folder_id}' in parents and trashed = false",
                    spaces='drive',
                    fields='nextPageToken, files(id, name, mimeType)',
                    pageToken=page_token
                ).execute()
                
                results.extend(response.get('files', []))
                page_token = response.get('nextPageToken')
                
                if not page_token:
                    break
            except Exception as e:
                print(f"Error listing files with standard method: {str(e)}")
                # If we hit an error, try with include permissions
                break
        
        # If the standard list didn't work, try with the sharedWithMe option
        if not results:
            print("Trying alternate method to list shared files...")
            try:
                # First, try to list files shared with me
                shared_response = service.files().list(
                    q="sharedWithMe=true",
                    spaces='drive',
                    fields='files(id, name, mimeType, parents)',
                    pageSize=100
                ).execute()
                
                # Filter for files within our folder or with the folder itself
                for file in shared_response.get('files', []):
                    # If this is the folder we're looking for
                    if file.get('id') == folder_id:
                        print(f"Found the folder: {file.get('name')} (ID: {folder_id})")
                    
                    # If this file is in the folder we're looking for
                    parents = file.get('parents', [])
                    if parents and folder_id in parents:
                        results.append(file)
            except Exception as e:
                print(f"Error listing shared files: {str(e)}")
    except Exception as e:
        print(f"Error in list_files_and_folders: {str(e)}")
    
    return results

def create_folder(service, name, parent_id):
    file_metadata = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_id]
    }
    
    folder = service.files().create(body=file_metadata, fields='id').execute()
    return folder.get('id')

def copy_file(service, file_id, name, parent_id):
    file_metadata = {
        'name': name,
        'parents': [parent_id]
    }
    
    file = service.files().copy(
        fileId=file_id,
        body=file_metadata,
        fields='id'
    ).execute()
    
    return file.get('id')

def process_folder(service, source_folder_id, dest_folder_id, path=""):
    items = list_files_and_folders(service, source_folder_id)
    total = len(items)
    
    print(f"Found {total} items in {path or 'root folder'}")
    
    for i, item in enumerate(items, 1):
        name = item['name']
        item_path = f"{path}/{name}" if path else name
        print(f"Processing {i}/{total}: {item_path}")
        
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            # Create a new folder
            new_folder_id = create_folder(service, name, dest_folder_id)
            print(f"Created folder: {item_path}")
            
            # Process items in this folder recursively
            process_folder(service, item['id'], new_folder_id, item_path)
        else:
            # Copy the file
            try:
                new_file_id = copy_file(service, item['id'], name, dest_folder_id)
                print(f"Copied file: {item_path}")
            except Exception as e:
                print(f"Error copying file {item_path}: {e}")

def main():
    # Parse command line arguments
    args = parse_arguments()
    
    # Set folder IDs from arguments or use defaults
    source_folder_id = args.source if args.source else DEFAULT_SOURCE_FOLDER_ID
    destination_folder_id = args.destination if args.destination else DEFAULT_DESTINATION_FOLDER_ID
    
    # Get credentials and build the service
    creds = get_credentials()
    service = build('drive', 'v3', credentials=creds)
    
    # Verify access to both folders
    print("\nVerifying folder access:")
    source_ok = verify_folder_access(service, source_folder_id, "Source folder")
    dest_ok = verify_folder_access(service, destination_folder_id, "Destination folder")
    
    if not source_ok or not dest_ok:
        print("\nPlease check the folder IDs and ensure you have access to both folders.")
        print(f"Source folder ID: {source_folder_id}")
        print(f"Destination folder ID: {destination_folder_id}")
        print("\nTip: Use the -s and -d flags to specify folder IDs directly:")
        print("python gdrive_copy.py -s SOURCE_FOLDER_ID -d DESTINATION_FOLDER_ID")
        return
    
    # Get source folder name
    source_folder = service.files().get(fileId=source_folder_id, fields='name').execute()
    source_folder_name = source_folder.get('name', 'Source folder')
    
    # Get destination folder name
    dest_folder = service.files().get(fileId=destination_folder_id, fields='name').execute()
    dest_folder_name = dest_folder.get('name', 'Destination folder')
    
    # Test: List source directory contents
    print(f"\nListing contents of source folder '{source_folder_name}':")
    source_items = list_files_and_folders(service, source_folder_id)
    
    if not source_items:
        print("No items found in source folder.")
    else:
        for item in source_items:
            item_type = "Folder" if item['mimeType'] == 'application/vnd.google-apps.folder' else "File"
            print(f"- {item_type}: {item['name']}")
    
    # Test: List destination directory contents
    print(f"\nListing contents of destination folder '{dest_folder_name}':")
    dest_items = list_files_and_folders(service, destination_folder_id)
    
    if not dest_items:
        print("No items found in destination folder.")
    else:
        for item in dest_items:
            item_type = "Folder" if item['mimeType'] == 'application/vnd.google-apps.folder' else "File"
            print(f"- {item_type}: {item['name']}")
    
    # Check if there are source items to copy
    if not source_items:
        print("\nNo items to copy from source folder. Operation cancelled.")
        return
    
    # If list-only mode, exit here
    if args.list_only:
        print("\nList-only mode. No files will be copied.")
        return
    
    # Determine if we should proceed automatically
    auto_proceed = args.yes
    
    if not auto_proceed:
        try:
            # Ask for confirmation before proceeding with the copy
            response = input(f"\nDo you want to copy all items from '{source_folder_name}' to '{dest_folder_name}'? (y/n): ")
            if response.lower() != 'y':
                print("Operation cancelled.")
                return
        except (EOFError, KeyboardInterrupt):
            # Handle non-interactive environments
            print("\nNon-interactive environment detected. Run with -y flag to proceed automatically.")
            print("Operation cancelled.")
            return
    
    print(f"\nStarting to copy from '{source_folder_name}' to '{dest_folder_name}'")
    print("This may take a while depending on the number and size of files...")
    
    process_folder(service, source_folder_id, destination_folder_id)
    
    print("Copy process completed!")

if __name__ == '__main__':
    main()