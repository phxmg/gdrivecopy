#!/usr/bin/env python3

import os
import pickle
import sys
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Define the required scopes
SCOPES = ['https://www.googleapis.com/auth/drive']

def print_color(text, color):
    """Print colored text."""
    colors = {
        'green': '\033[92m',
        'yellow': '\033[93m',
        'red': '\033[91m',
        'blue': '\033[94m',
        'cyan': '\033[96m',
        'magenta': '\033[95m',
        'bold': '\033[1m',
        'end': '\033[0m'
    }
    try:
        print(f"{colors.get(color, '')}{text}{colors['end']}")
    except:
        print(text)

def get_credentials():
    """Get and refresh Google API credentials."""
    creds = None
    
    # Token file stores the user's access and refresh tokens
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    # If there are no valid credentials, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return creds

def list_all_folders(service):
    """List all folders in My Drive, shared drives, and shared with you."""
    # 1. List folders in My Drive
    print_color("\nListing folders in your My Drive...", 'blue')
    try:
        response = service.files().list(
            q="'root' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
            spaces='drive',
            fields='files(id, name)',
            pageSize=100
        ).execute()
        
        items = response.get('files', [])
        print_color(f"Found {len(items)} folders in My Drive", 'green')
        
        if items:
            for i, item in enumerate(items, 1):
                print(f"{i}. 🗂️ {item.get('name')} [ID: {item.get('id')}]")
    except Exception as e:
        print_color(f"Error listing My Drive folders: {str(e)}", 'red')
    
    # 2. List shared with me folders
    print_color("\nListing folders shared with you...", 'blue')
    try:
        shared_response = service.files().list(
            q="sharedWithMe=true and mimeType='application/vnd.google-apps.folder'",
            spaces='drive',
            fields='files(id, name, sharingUser)',
            pageSize=100
        ).execute()
        
        shared_items = shared_response.get('files', [])
        print_color(f"Found {len(shared_items)} folders shared with you", 'green')
        
        if shared_items:
            for i, item in enumerate(shared_items, 1):
                # Get sharing user if available
                shared_by = ""
                if 'sharingUser' in item and 'displayName' in item['sharingUser']:
                    shared_by = f" [Shared by: {item['sharingUser']['displayName']}]"
                
                print(f"{i}. 👤 {item.get('name')}{shared_by} [ID: {item.get('id')}]")
    except Exception as e:
        print_color(f"Error listing shared folders: {str(e)}", 'red')
    
    # 3. List shared drives
    print_color("\nListing shared drives...", 'blue')
    try:
        drives_response = service.drives().list(pageSize=50).execute()
        drives = drives_response.get('drives', [])
        
        print_color(f"Found {len(drives)} shared drives", 'green')
        
        if drives:
            for i, drive in enumerate(drives, 1):
                print(f"{i}. 👥 {drive.get('name')} [ID: {drive.get('id')}]")
    except Exception as e:
        print_color(f"Error listing shared drives: {str(e)}", 'yellow')
    
    # 4. Search for "Gateway" folders specifically
    print_color("\nSearching for Gateway folders...", 'blue')
    try:
        gateway_response = service.files().list(
            q="name contains 'Gateway' and mimeType='application/vnd.google-apps.folder'",
            fields="files(id, name)",
            spaces='drive',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=20
        ).execute()
        
        gateway_items = gateway_response.get('files', [])
        print_color(f"Found {len(gateway_items)} Gateway folders", 'green')
        
        if gateway_items:
            for i, item in enumerate(gateway_items, 1):
                print(f"{i}. 🔍 {item.get('name')} [ID: {item.get('id')}]")
    except Exception as e:
        print_color(f"Error searching for Gateway folders: {str(e)}", 'red')

def get_user_info(service):
    """Get information about the authenticated user."""
    try:
        user_info = service.about().get(fields="user").execute()
        display_name = user_info.get('user', {}).get('displayName', 'Unknown')
        email = user_info.get('user', {}).get('emailAddress', 'Unknown')
        
        print_color(f"\nAuthenticated as:", 'cyan')
        print_color(f"Name: {display_name}", 'cyan')
        print_color(f"Email: {email}", 'cyan')
        return display_name, email
    except Exception as e:
        print_color(f"Error getting user info: {str(e)}", 'red')
        return "Unknown", "Unknown"

def main():
    print_color("=== GOOGLE DRIVE FOLDER LISTER ===", 'blue')
    print("This tool lists folders in your Google Drive and shows account information.")
    
    try:
        # Get credentials and build service
        creds = get_credentials()
        service = build('drive', 'v3', credentials=creds)
        print_color("✅ Successfully connected to Google Drive", 'green')
        
        # Display user info to confirm identity
        get_user_info(service)
        
        # List all folders from different sources
        list_all_folders(service)
        
    except Exception as e:
        print_color(f"\n❌ Error: {str(e)}", 'red')
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())