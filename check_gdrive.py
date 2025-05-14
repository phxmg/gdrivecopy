#!/usr/bin/env python3

import os
import pickle
import sys
import time
import argparse
import random
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError

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

def retry_with_exponential_backoff(func, max_retries=8, initial_delay=10, factor=2, jitter=0.1):
    """Retry a function with exponential backoff for rate limiting issues."""
    def wrapper(*args, **kwargs):
        retry_count = 0
        delay = initial_delay
        
        while True:
            try:
                # No delay for successful operations - only delay when we hit rate limits
                return func(*args, **kwargs)
            except HttpError as e:
                # Check if this is a rate limit error
                if e.resp.status == 403 and "userRateLimitExceeded" in str(e):
                    retry_count += 1
                    if retry_count > max_retries:
                        # We've exhausted our retries, re-raise the exception
                        raise
                    
                    # Calculate the delay with some randomness
                    sleep_time = delay * (1 + random.uniform(-jitter, jitter))
                    
                    # Log the rate limit and retry
                    print_color(f"  Rate limit hit. Retrying in {sleep_time:.1f} seconds (attempt {retry_count}/{max_retries})...", 'yellow')
                    
                    # Sleep before retrying
                    time.sleep(sleep_time)
                    
                    # Increase the delay for next time
                    delay = min(delay * factor, 300)  # Cap at 5 minutes
                else:
                    # This is not a rate limit error, re-raise
                    raise
            except Exception as e:
                # For non-HTTP errors, just re-raise
                raise
    
    return wrapper

def get_folder_contents(service, folder_id, is_shared_drive=False):
    """Get contents of a folder (non-recursive)."""
    results = []
    page_token = None
    
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        
        # Define a function to list files with retry capability
        @retry_with_exponential_backoff
        def list_files_with_retry(is_shared=False, token=None):
            if is_shared:
                return service.files().list(
                    q=query,
                    spaces='drive',
                    fields='nextPageToken, files(id, name, mimeType, size)',
                    pageToken=token,
                    pageSize=100,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    driveId=folder_id,
                    corpora='drive'
                ).execute()
            else:
                return service.files().list(
                    q=query,
                    spaces='drive',
                    fields='nextPageToken, files(id, name, mimeType, size)',
                    pageToken=token,
                    pageSize=100,
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True
                ).execute()
        
        # For shared drives, we need special handling
        if is_shared_drive:
            while True:
                response = list_files_with_retry(is_shared=True, token=page_token)
                
                items = response.get('files', [])
                results.extend(items)
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
        else:
            # Standard handling for regular folders
            while True:
                response = list_files_with_retry(is_shared=False, token=page_token)
                
                items = response.get('files', [])
                results.extend(items)
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
    except Exception as e:
        print_color(f"Error listing folder contents: {str(e)}", 'red')
    
    return results

def scan_folder_recursive(service, folder_id, folder_name, is_shared_drive=False, max_depth=None, current_depth=0, stats=None):
    """Recursively scan a folder structure and collect statistics."""
    # Initialize stats dictionary if not provided
    if stats is None:
        stats = {
            'total_folders': 0,
            'total_files': 0,
            'total_size_bytes': 0,
            'largest_files': [],  # Will keep track of largest files [size, name, path]
            'folders_by_size': {},  # Will track folder sizes {folder_path: size}
            'files_by_extension': {},  # Will track file counts by extension {ext: count}
            'folder_tree': {},  # Will store folder structure
            'file_details': [],  # Will store detailed info about each file for comparison later
        }
    
    # Get all folder contents
    items = get_folder_contents(service, folder_id, is_shared_drive)
    
    # Process folders first
    folders = [item for item in items if item.get('mimeType') == 'application/vnd.google-apps.folder']
    files = [item for item in items if item.get('mimeType') != 'application/vnd.google-apps.folder']
    
    # Update folder count
    stats['total_folders'] += len(folders)
    stats['total_files'] += len(files)
    
    # Process files
    folder_size = 0
    for file in files:
        # Get file size
        file_size = int(file.get('size', 0))
        folder_size += file_size
        stats['total_size_bytes'] += file_size
        
        # Track file by extension
        file_name = file.get('name', '')
        ext = file_name.split('.')[-1].lower() if '.' in file_name else 'none'
        stats['files_by_extension'][ext] = stats['files_by_extension'].get(ext, 0) + 1
        
        # Track largest files (keep top 10)
        file_path = f"{folder_name}/{file_name}"
        stats['largest_files'].append((file_size, file_name, file_path))
        stats['largest_files'] = sorted(stats['largest_files'], key=lambda x: x[0], reverse=True)[:10]
        
        # Store detailed file info for later comparison
        stats['file_details'].append({
            'id': file.get('id'),
            'name': file_name,
            'path': file_path,
            'size': file_size,
            'mime_type': file.get('mimeType', ''),
            'parent_folder_id': folder_id,
            'folder_path': folder_name
        })
    
    # Store current folder info in the tree
    current_folder = {
        'id': folder_id,
        'name': folder_name.split('/')[-1] if '/' in folder_name else folder_name,
        'files': len(files),
        'folders': len(folders),
        'size': folder_size,
        'children': {}
    }
    
    # Add to folder tree (creating path if needed)
    if '/' in folder_name:
        # This is a subfolder, add to parent
        path_parts = folder_name.split('/')
        parent_path = '/'.join(path_parts[:-1])
        current_folder_name = path_parts[-1]
        
        # Navigate to parent folder in tree
        current_node = stats['folder_tree']
        for part in path_parts[:-1]:
            if part in current_node:
                current_node = current_node[part]['children']
            else:
                # This shouldn't happen, but handle it gracefully
                current_node[part] = {'children': {}}
                current_node = current_node[part]['children']
        
        # Add current folder to parent
        current_node[current_folder_name] = current_folder
    else:
        # This is a root folder
        stats['folder_tree'][folder_name] = current_folder
    
    # Store folder size for statistics
    stats['folders_by_size'][folder_name] = folder_size
    
    # Stop recursion if we've reached the maximum depth
    if max_depth is not None and current_depth >= max_depth:
        return stats
    
    # Process subfolders recursively
    for folder in folders:
        subfolder_name = folder.get('name')
        subfolder_path = f"{folder_name}/{subfolder_name}"
        scan_folder_recursive(
            service, folder.get('id'), subfolder_path, is_shared_drive,
            max_depth, current_depth + 1, stats
        )
    
    return stats

def format_size(size_bytes):
    """Format size in bytes to human-readable format."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

def display_folder_stats(stats):
    """Display formatted folder statistics."""
    print_color("\n====== FOLDER STATISTICS ======", 'blue')
    print(f"Total Folders: {stats['total_folders']}")
    print(f"Total Files: {stats['total_files']}")
    print(f"Total Size: {format_size(stats['total_size_bytes'])}")
    
    # Display file extensions
    if stats['files_by_extension']:
        print_color("\nFile Types:", 'blue')
        sorted_exts = sorted(stats['files_by_extension'].items(), key=lambda x: x[1], reverse=True)
        for ext, count in sorted_exts[:10]:  # Show top 10 extensions
            print(f"  .{ext}: {count} files")
    
    # Display largest files
    if stats['largest_files']:
        print_color("\nLargest Files:", 'blue')
        for size, name, path in stats['largest_files'][:5]:  # Show top 5 largest files
            print(f"  {name} ({format_size(size)})")
    
    # Display largest folders
    if stats['folders_by_size']:
        print_color("\nLargest Folders:", 'blue')
        sorted_folders = sorted(stats['folders_by_size'].items(), key=lambda x: x[1], reverse=True)
        for folder_path, size in sorted_folders[:5]:  # Show top 5 largest folders
            if size > 0:  # Only show non-empty folders
                folder_name = folder_path.split('/')[-1] if '/' in folder_path else folder_path
                print(f"  {folder_name} ({format_size(size)})")
    
    print_color("\nFolder Structure:", 'blue')
    
    def print_folder_tree(tree, prefix="  "):
        """Recursively print folder tree with proper indentation."""
        for i, (folder_name, folder_info) in enumerate(sorted(tree.items())):
            files_count = folder_info.get('files', 0)
            folders_count = folder_info.get('folders', 0)
            size = folder_info.get('size', 0)
            
            # Check if this is the last item at this level
            is_last = i == len(tree) - 1
            
            # Print current folder with proper connector
            print(f"{prefix}{'└── ' if is_last else '├── '}📁 {folder_name} ({folders_count} folders, {files_count} files, {format_size(size)})")
            
            # Prepare prefix for children
            child_prefix = prefix + ('    ' if is_last else '│   ')
            
            # Recurse into child folders if any
            if folder_info.get('children'):
                print_folder_tree(folder_info.get('children', {}), prefix=child_prefix)
    
    # Start printing the tree
    print_folder_tree(stats['folder_tree'])
    
    print_color("\n==============================", 'blue')

def list_all_folders(service):
    """List all folders in My Drive, shared drives, and shared with me."""
    all_folders = []
    folder_sources = []
    
    # 1. List folders in My Drive
    print_color("\nListing folders in your My Drive...", 'blue')
    try:
        # First, get the root folder ID 'My Drive'
        root_folder = service.files().get(fileId='root', fields='id, name').execute()
        root_folder['source'] = 'My Drive'
        all_folders.append(root_folder)
        folder_sources.append('My Drive')
        
        # Get top-level folders in My Drive
        response = service.files().list(
            q="'root' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
            spaces='drive',
            fields='files(id, name)',
            pageSize=100
        ).execute()
        
        for item in response.get('files', []):
            item['source'] = 'My Drive'
            all_folders.append(item)
        
        print_color(f"Found {len(response.get('files', []))} folders in My Drive", 'green')
    except Exception as e:
        print_color(f"Error listing My Drive folders: {str(e)}", 'red')
    
    # 2. List shared drives
    print_color("\nListing shared drives...", 'blue')
    try:
        drives_response = service.drives().list(pageSize=50).execute()
        drives = drives_response.get('drives', [])
        
        for drive in drives:
            drive_info = {
                'id': drive.get('id'),
                'name': drive.get('name'),
                'source': 'Shared Drive',
                'isSharedDrive': True
            }
            all_folders.append(drive_info)
            folder_sources.append('Shared Drive')
        
        print_color(f"Found {len(drives)} shared drives", 'green')
    except Exception as e:
        print_color(f"Error listing shared drives: {str(e)}", 'yellow')
    
    # 3. List shared with me folders
    print_color("\nListing folders shared with you...", 'blue')
    try:
        shared_response = service.files().list(
            q="sharedWithMe=true and mimeType='application/vnd.google-apps.folder'",
            spaces='drive',
            fields='files(id, name, sharingUser)',
            pageSize=100
        ).execute()
        
        shared_items = shared_response.get('files', [])
        
        for item in shared_items:
            # Get sharing user if available
            shared_by = ""
            if 'sharingUser' in item and 'displayName' in item['sharingUser']:
                shared_by = item['sharingUser']['displayName']
            
            item['source'] = 'Shared with me'
            item['sharedBy'] = shared_by
            all_folders.append(item)
            folder_sources.append('Shared with me')
        
        print_color(f"Found {len(shared_items)} folders shared with you", 'green')
    except Exception as e:
        print_color(f"Error listing shared folders: {str(e)}", 'red')
    
    # 4. Search for specific folders by name
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
        
        # Check if items are already in our list
        existing_ids = [item['id'] for item in all_folders]
        gateway_added = 0
        
        for item in gateway_items:
            if item['id'] not in existing_ids:
                item['source'] = 'Search result'
                all_folders.append(item)
                existing_ids.append(item['id'])
                folder_sources.append('Search result')
                gateway_added += 1
        
        print_color(f"Found {gateway_added} additional Gateway folders through search", 'green')
    except Exception as e:
        print_color(f"Error searching for Gateway folders: {str(e)}", 'red')
    
    # Display the results
    print_color(f"\nFound {len(all_folders)} total folders across all sources", 'green')
    
    # Create a set of unique sources for the summary
    unique_sources = set(folder_sources)
    source_counts = {source: folder_sources.count(source) for source in unique_sources}
    
    print_color("Summary by source:", 'blue')
    for source, count in source_counts.items():
        print(f"  {source}: {count} folders")
    
    # Display all folders
    print_color("\nAll available folders:", 'blue')
    for i, item in enumerate(all_folders, 1):
        # Format the display based on the source
        source = item.get('source', 'Unknown')
        source_icon = ""
        source_info = ""
        
        if source == 'My Drive':
            source_icon = "🗂️"
        elif source == 'Shared Drive':
            source_icon = "👥"
        elif source == 'Shared with me':
            source_icon = "👤"
            if 'sharedBy' in item and item['sharedBy']:
                source_info = f" [Shared by: {item['sharedBy']}]"
        elif source == 'Search result':
            source_icon = "🔍"
        
        # Print with index for selection
        print(f"{i}. {source_icon} {item.get('name')} ({source}){source_info} [ID: {item.get('id')}]")
    
    return all_folders

def main():
    print_color("=== GOOGLE DRIVE FOLDER ANALYZER ===", 'blue')
    print("This tool lists folders in your Google Drive and analyzes their contents.")
    
    try:
        # Get credentials and build service
        creds = get_credentials()
        service = build('drive', 'v3', credentials=creds)
        print_color("✅ Successfully connected to Google Drive", 'green')
        
        # List all folders from different sources
        all_folders = list_all_folders(service)
        
        if not all_folders:
            print_color("No folders found.", 'yellow')
            return 1
        
        # Let user select a folder
        while True:
            try:
                choice = input("\nSelect folder number to analyze (or 'q' to quit): ")
                
                if choice.lower() == 'q':
                    print_color("Exiting...", 'yellow')
                    return 0
                
                index = int(choice) - 1
                if 0 <= index < len(all_folders):
                    selected_folder = all_folders[index]
                    break
                else:
                    print_color("Invalid selection. Please try again.", 'red')
            except ValueError:
                print_color("Please enter a number or 'q'.", 'red')
        
        # Get folder details
        folder_id = selected_folder.get('id')
        folder_name = selected_folder.get('name')
        is_shared_drive = selected_folder.get('isSharedDrive', False)
        
        print_color(f"\n📁 Selected folder: {folder_name} [ID: {folder_id}]", 'blue')
        
        # Start scanning folder recursively to get stats
        print_color("\nScanning folder structure and calculating sizes...", 'blue')
        print("This may take a moment depending on the folder size and depth...")
        
        # Scan recursively and get stats
        stats = scan_folder_recursive(service, folder_id, folder_name, is_shared_drive)
        
        # Display folder statistics
        display_folder_stats(stats)
        
    except Exception as e:
        print_color(f"\n❌ Error: {str(e)}", 'red')
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())