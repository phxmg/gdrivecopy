#!/usr/bin/env python3

import os
import pickle
import sys
import time
import json
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Define the required scopes
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.readonly']

# Target folder ID from URL
TARGET_FOLDER_ID = "1YdkvUODE7r7KYHe7FtjxFbb3Uy-ecqA7"  # Gateway Footage

# Debug flag - set to True for verbose output
DEBUG = True

def debug_print(message, obj=None):
    """Print debug messages when DEBUG is True."""
    if DEBUG:
        print(f"\n[DEBUG] {message}")
        if obj is not None:
            try:
                if isinstance(obj, dict) or isinstance(obj, list):
                    print(json.dumps(obj, indent=2))
                else:
                    print(obj)
            except:
                print(f"Could not serialize object: {type(obj)}")

def get_credentials():
    """Get and refresh Google API credentials."""
    creds = None
    
    # Token file stores the user's access and refresh tokens
    if os.path.exists('token.pickle'):
        debug_print("Loading credentials from token.pickle")
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    # If there are no valid credentials, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            debug_print("Refreshing expired credentials")
            creds.refresh(Request())
        else:
            debug_print("Getting new credentials")
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return creds

def direct_folder_access(service, folder_id):
    """Attempt to directly access a folder by ID."""
    print(f"\n--- DIRECT FOLDER ACCESS ---")
    print(f"Trying to access folder with ID: {folder_id}")
    
    try:
        # Try direct access
        folder = service.files().get(
            fileId=folder_id,
            fields='id,name,mimeType,capabilities,owners,shared,sharingUser',
            supportsAllDrives=True
        ).execute()
        
        print(f"✅ SUCCESS! Found folder: {folder.get('name', 'Unknown')}")
        print(f"  ID: {folder.get('id')}")
        print(f"  Type: {folder.get('mimeType')}")
        
        if 'capabilities' in folder:
            caps = folder['capabilities']
            print(f"  Capabilities:")
            for key, value in caps.items():
                print(f"    {key}: {value}")
        
        # Try to list contents
        try:
            print(f"\nListing contents of folder...")
            items = list_folder_contents(service, folder_id)
            
            if items:
                print(f"✅ SUCCESS! Found {len(items)} items in folder:")
                for i, item in enumerate(items[:10], 1):  # Show first 10
                    item_type = "📁 Folder" if item.get('mimeType') == 'application/vnd.google-apps.folder' else "📄 File"
                    print(f"  {i}. {item_type}: {item.get('name')} [ID: {item.get('id')}]")
                
                if len(items) > 10:
                    print(f"  ... and {len(items) - 10} more items")
            else:
                print("❌ No items found in folder (empty or no access to contents)")
        except Exception as e:
            print(f"❌ Error listing folder contents: {str(e)}")
        
        return folder
    except Exception as e:
        print(f"❌ Error accessing folder: {str(e)}")
        return None

def check_shared_with_me(service):
    """Check items shared with the user."""
    print(f"\n--- SHARED WITH ME CHECK ---")
    print(f"Checking items shared with you...")
    
    try:
        response = service.files().list(
            q="sharedWithMe=true",
            fields="files(id,name,mimeType,owners,sharingUser)",
            pageSize=30
        ).execute()
        
        items = response.get('files', [])
        
        if items:
            print(f"✅ Found {len(items)} items shared with you:")
            folders = [item for item in items if item.get('mimeType') == 'application/vnd.google-apps.folder']
            files = [item for item in items if item.get('mimeType') != 'application/vnd.google-apps.folder']
            
            print(f"\n  Shared Folders ({len(folders)}):")
            for i, folder in enumerate(folders, 1):
                shared_by = folder.get('sharingUser', {}).get('displayName', 'Unknown')
                print(f"  {i}. 📁 {folder.get('name')} [Shared by: {shared_by}] [ID: {folder.get('id')}]")
            
            print(f"\n  Shared Files ({len(files)}):")
            for i, file in enumerate(files[:5], 1):  # Just show first 5 files
                shared_by = file.get('sharingUser', {}).get('displayName', 'Unknown')
                print(f"  {i}. 📄 {file.get('name')} [Shared by: {shared_by}] [ID: {file.get('id')}]")
            
            if len(files) > 5:
                print(f"  ... and {len(files) - 5} more files")
                
            # Check if target folder is in the list
            target_items = [item for item in items if item.get('id') == TARGET_FOLDER_ID]
            if target_items:
                print(f"\n✅ TARGET FOLDER FOUND in shared items!")
                for item in target_items:
                    print(f"  📁 {item.get('name')} [ID: {item.get('id')}]")
            else:
                print(f"\n❌ Target folder NOT found in shared items")
        else:
            print("❌ No items found shared with you")
        
        return items
    except Exception as e:
        print(f"❌ Error checking shared items: {str(e)}")
        return []

def check_all_accessible_folders(service):
    """Try to list all folders the user can access."""
    print(f"\n--- ALL ACCESSIBLE FOLDERS ---")
    print(f"Listing folders you can access...")
    
    try:
        response = service.files().list(
            q="mimeType='application/vnd.google-apps.folder'",
            fields="files(id,name,parents,shared)",
            pageSize=100
        ).execute()
        
        items = response.get('files', [])
        
        if items:
            print(f"✅ Found {len(items)} folders you can access")
            
            # Look for target folder
            target_items = [item for item in items if item.get('id') == TARGET_FOLDER_ID]
            if target_items:
                print(f"\n✅ TARGET FOLDER FOUND in accessible folders!")
                for item in target_items:
                    print(f"  📁 {item.get('name')} [ID: {item.get('id')}]")
                    
                    # Try to get parent info
                    if 'parents' in item and item['parents']:
                        for parent_id in item['parents']:
                            try:
                                parent = service.files().get(
                                    fileId=parent_id,
                                    fields='id,name,mimeType',
                                    supportsAllDrives=True
                                ).execute()
                                print(f"  └── Parent: 📁 {parent.get('name')} [ID: {parent.get('id')}]")
                            except Exception as e:
                                print(f"  └── Parent ID: {parent_id} (Error: {str(e)})")
            else:
                print(f"❌ Target folder NOT found in accessible folders")
                
            # Look for Gateway in name
            gateway_items = [item for item in items if 'gateway' in item.get('name', '').lower()]
            if gateway_items:
                print(f"\nFound {len(gateway_items)} folders with 'gateway' in the name:")
                for i, item in enumerate(gateway_items, 1):
                    print(f"  {i}. 📁 {item.get('name')} [ID: {item.get('id')}]")
        else:
            print("❌ No folders found")
        
        return items
    except Exception as e:
        print(f"❌ Error listing accessible folders: {str(e)}")
        return []

def check_shared_drives(service):
    """Check shared drives and their contents."""
    print(f"\n--- SHARED DRIVES CHECK ---")
    print(f"Checking shared drives...")
    
    try:
        drives_response = service.drives().list(pageSize=50).execute()
        drives = drives_response.get('drives', [])
        
        if drives:
            print(f"✅ Found {len(drives)} shared drives:")
            for i, drive in enumerate(drives, 1):
                print(f"  {i}. 📁 {drive.get('name')} [ID: {drive.get('id')}]")
                
                # Look for the target folder in this drive
                try:
                    print(f"    Searching for target folder in this drive...")
                    response = service.files().list(
                        q=f"mimeType='application/vnd.google-apps.folder'",
                        spaces='drive',
                        fields="files(id,name,mimeType)",
                        driveId=drive.get('id'),
                        includeItemsFromAllDrives=True,
                        supportsAllDrives=True,
                        corpora='drive',
                        pageSize=100
                    ).execute()
                    
                    items = response.get('files', [])
                    if items:
                        # Look for exact match
                        target_items = [item for item in items if item.get('id') == TARGET_FOLDER_ID]
                        if target_items:
                            print(f"    ✅ TARGET FOLDER FOUND in this shared drive!")
                            for item in target_items:
                                print(f"    └── 📁 {item.get('name')} [ID: {item.get('id')}]")
                        
                        # Look for Gateway Footage by name
                        gw_items = [item for item in items if item.get('name') == 'Gateway Footage']
                        if gw_items:
                            print(f"    ✅ Found {len(gw_items)} 'Gateway Footage' folders in this drive:")
                            for j, item in enumerate(gw_items, 1):
                                print(f"    └── {j}. 📁 {item.get('name')} [ID: {item.get('id')}]")
                    else:
                        print(f"    No folders found in this drive")
                except Exception as e:
                    print(f"    ❌ Error searching drive: {str(e)}")
        else:
            print("❌ No shared drives found")
        
        return drives
    except Exception as e:
        print(f"❌ Error checking shared drives: {str(e)}")
        return []

def search_by_name(service, name):
    """Search for a folder by name."""
    print(f"\n--- SEARCH BY NAME ---")
    print(f"Searching for folders named '{name}'...")
    
    try:
        response = service.files().list(
            q=f"name='{name}' and mimeType='application/vnd.google-apps.folder'",
            fields="files(id,name,mimeType,owners,sharingUser,shared)",
            spaces='drive',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=10
        ).execute()
        
        items = response.get('files', [])
        
        if items:
            print(f"✅ Found {len(items)} folders named '{name}':")
            for i, item in enumerate(items, 1):
                shared_info = ""
                if 'sharingUser' in item and 'displayName' in item['sharingUser']:
                    shared_info = f" [Shared by: {item['sharingUser']['displayName']}]"
                print(f"  {i}. 📁 {item.get('name')}{shared_info} [ID: {item.get('id')}]")
        else:
            print(f"❌ No folders found with name '{name}'")
        
        return items
    except Exception as e:
        print(f"❌ Error searching by name: {str(e)}")
        return []

def list_folder_contents(service, folder_id):
    """List contents of a folder."""
    results = []
    page_token = None
    
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        
        while True:
            response = service.files().list(
                q=query,
                spaces='drive',
                fields='nextPageToken, files(id, name, mimeType, size)',
                pageToken=page_token,
                pageSize=100,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute()
            
            items = response.get('files', [])
            results.extend(items)
            
            page_token = response.get('nextPageToken')
            if not page_token:
                break
    except Exception as e:
        debug_print(f"Error listing folder contents: {str(e)}")
    
    return results

def main():
    print("====== GOOGLE DRIVE FOLDER DEBUG TOOL ======")
    print("This script will help locate the 'Gateway Footage' folder.")
    print(f"Target Folder ID: {TARGET_FOLDER_ID}")
    
    try:
        # Get credentials and build service
        creds = get_credentials()
        service = build('drive', 'v3', credentials=creds)
        print("\n✅ Successfully connected to Google Drive")
        
        # 1. Try direct access to the folder
        direct_folder_access(service, TARGET_FOLDER_ID)
        
        # 2. Search for folders shared with the user
        check_shared_with_me(service)
        
        # 3. Search for the folder by name
        search_by_name(service, "Gateway Footage")
        
        # 4. Check all accessible folders
        check_all_accessible_folders(service)
        
        # 5. Check shared drives
        check_shared_drives(service)
        
        print("\n====== DEBUG COMPLETE ======")
        print("If the folder was found anywhere above, note its location and ID.")
        print("Use that information when running the main script.")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())