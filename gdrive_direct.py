#!/usr/bin/env python3

import os
import pickle
import sys
import time
import argparse
import random
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError

# Define the required scopes
SCOPES = ['https://www.googleapis.com/auth/drive']

# Direct folder IDs (from debugging)
SOURCE_FOLDER_ID = "1YdkvUODE7r7KYHe7FtjxFbb3Uy-ecqA7"  # Gateway Footage
DEFAULT_DESTINATION_ID = "1IXsLFhcDRUhe_c-Kbm_GeLqI6z7UrB5I"  # Gen Gateway

def print_color(text, color):
    """Print colored text."""
    colors = {
        'green': '\033[92m',
        'yellow': '\033[93m',
        'red': '\033[91m',
        'blue': '\033[94m',
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

def get_folder_name(service, folder_id):
    """Get the name of a folder by ID."""
    try:
        folder = service.files().get(
            fileId=folder_id,
            fields='name',
            supportsAllDrives=True
        ).execute()
        return folder.get('name', 'Unknown Folder')
    except Exception as e:
        print_color(f"Error getting folder name: {str(e)}", 'red')
        return "Unknown Folder"

def list_shared_folders(service):
    """List folders shared with the user and let them pick one."""
    print_color("\nFinding folders shared with you:", 'blue')
    
    try:
        # Method 1: Get shared folders directly marked as sharedWithMe
        shared_items = []
        page_token = None
        
        while True:
            response = service.files().list(
                q="sharedWithMe=true and mimeType='application/vnd.google-apps.folder'",
                spaces='drive',
                fields='nextPageToken, files(id, name, mimeType, owners, shared, sharingUser)',
                pageToken=page_token,
                pageSize=100
            ).execute()
            
            items = response.get('files', [])
            shared_items.extend(items)
            
            page_token = response.get('nextPageToken')
            if not page_token:
                break
        
        # Method 2: Search by name for "Gateway" folders
        gateway_response = service.files().list(
            q="name contains 'Gateway' and mimeType='application/vnd.google-apps.folder'",
            fields="files(id, name, mimeType, owners, sharingUser, shared)",
            spaces='drive',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageSize=20
        ).execute()
        
        gateway_items = gateway_response.get('files', [])
        
        # Add Gateway items avoiding duplicates
        existing_ids = [item['id'] for item in shared_items]
        for item in gateway_items:
            if item['id'] not in existing_ids:
                item['specialSearch'] = True
                shared_items.append(item)
                existing_ids.append(item['id'])
        
        # Method 3: Get shared drives
        try:
            drives_response = service.drives().list(pageSize=50).execute()
            drives = drives_response.get('drives', [])
            
            for drive in drives:
                shared_items.append({
                    'id': drive.get('id'),
                    'name': f"{drive.get('name')} (Shared Drive)",
                    'mimeType': 'application/vnd.google-apps.folder',
                    'isSharedDrive': True
                })
        except Exception as e:
            print_color(f"Note: Could not access shared drives: {str(e)}", 'yellow')
        
        # Add the target Gateway Footage folder if not already in the list
        target_gateway_id = "1YdkvUODE7r7KYHe7FtjxFbb3Uy-ecqA7"
        if target_gateway_id not in existing_ids:
            try:
                folder = service.files().get(
                    fileId=target_gateway_id,
                    fields='id,name,mimeType',
                    supportsAllDrives=True
                ).execute()
                
                if folder and folder.get('mimeType') == 'application/vnd.google-apps.folder':
                    folder['directAccess'] = True
                    shared_items.append(folder)
            except Exception:
                pass  # If we can't access it, don't add it
        
        # Display the results
        if not shared_items:
            print_color("No shared folders found.", 'yellow')
            return None
        
        print_color(f"Found {len(shared_items)} folders:", 'green')
        
        # Display folders
        for i, item in enumerate(shared_items, 1):
            # Check if it's a shared drive
            is_shared_drive = item.get('isSharedDrive', False)
            
            # Get sharing user if available and not a shared drive
            shared_by = ""
            if not is_shared_drive and 'sharingUser' in item and 'displayName' in item['sharingUser']:
                shared_by = f" [Shared by: {item['sharingUser']['displayName']}]"
            
            # Add special markers
            special_marker = ""
            if is_shared_drive:
                shared_by = " [Shared Drive]"
            elif item.get('specialSearch', False):
                special_marker = " 🔍"  # Special search result
            elif item.get('directAccess', False):
                special_marker = " ⭐"  # Direct access
            
            # Highlight the default source (Gateway Footage)
            highlight = " (Default)" if item.get('id') == target_gateway_id else ""
            
            # Print with index for selection
            print(f"{i}. 📁 {item.get('name')}{special_marker}{shared_by}{highlight} [ID: {item.get('id')}]")
        
        # Let user select a folder
        while True:
            try:
                choice = input("\nSelect source folder number (or press Enter for default): ")
                
                if not choice.strip():
                    # Return the Gateway Footage folder if available
                    for item in shared_items:
                        if item.get('id') == target_gateway_id:
                            print_color(f"Using default: {item.get('name')} [ID: {item.get('id')}]", 'green')
                            return item
                    
                    # If no default found, use the first folder
                    print_color(f"Default folder not found, using: {shared_items[0].get('name')}", 'yellow')
                    return shared_items[0]
                
                index = int(choice) - 1
                if 0 <= index < len(shared_items):
                    print_color(f"Selected: {shared_items[index].get('name')}", 'green')
                    return shared_items[index]
                else:
                    print_color("Invalid selection. Please try again.", 'red')
            except ValueError:
                print_color("Please enter a number or press Enter.", 'red')
        
    except Exception as e:
        print_color(f"Error listing shared folders: {str(e)}", 'red')
        return None

def get_folder_contents(service, folder_id, is_shared_drive=False, skip_macos_resource_files=False):
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
    
    # No filtering for macOS resource fork files - return all files
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
            'skipped_resource_files': 0  # Count of skipped macOS resource fork files
        }
    
    # Get all folder contents (including macOS resource files)
    items = get_folder_contents(service, folder_id, is_shared_drive, skip_macos_resource_files=False)
    
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
    
    # No longer skipping macOS resource files
    
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

def list_destination_folders(service):
    """List folders in My Drive for destination selection."""
    print_color("\nFinding folders in your Drive for destination:", 'blue')
    
    try:
        # First, get the root folder ID 'My Drive'
        root_folder = service.files().get(fileId='root', fields='id, name').execute()
        
        # Get folders in the user's My Drive
        response = service.files().list(
            q="'root' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
            spaces='drive',
            fields='files(id, name)',
            pageSize=100
        ).execute()
        
        items = [root_folder] + response.get('files', [])
        
        if not items:
            print_color("No folders found in your My Drive.", 'yellow')
            return None
        
        print_color(f"Found {len(items)} folders in your Drive:", 'green')
        
        for i, item in enumerate(items, 1):
            folder_name = item.get('name', 'Unknown')
            folder_id = item.get('id', '')
            
            # Highlight the default destination
            highlight = " (Default)" if folder_id == DEFAULT_DESTINATION_ID else ""
            print(f"{i}. 📁 {folder_name}{highlight} [ID: {folder_id}]")
        
        while True:
            try:
                choice = input("\nSelect destination folder number (or press Enter for default): ")
                
                if not choice.strip():
                    # Use default destination
                    for item in items:
                        if item.get('id') == DEFAULT_DESTINATION_ID:
                            print_color(f"Using default: {item.get('name')} [ID: {item.get('id')}]", 'green')
                            return item
                    
                    # If no default found, use the first folder
                    print_color(f"Default folder not found, using: {items[0].get('name')}", 'yellow')
                    return items[0]
                
                index = int(choice) - 1
                if 0 <= index < len(items):
                    print_color(f"Selected: {items[index].get('name')}", 'green')
                    return items[index]
                else:
                    print_color("Invalid selection. Please try again.", 'red')
            except ValueError:
                print_color("Please enter a number or press Enter.", 'red')
    
    except Exception as e:
        print_color(f"Error listing destination folders: {str(e)}", 'red')
        return None

def create_folder(service, name, parent_id):
    """Create a new folder."""
    file_metadata = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_id]
    }
    
    try:
        folder = service.files().create(body=file_metadata, fields='id, name').execute()
        return folder
    except Exception as e:
        print_color(f"Error creating folder '{name}': {str(e)}", 'red')
        return None

def check_file_exists(service, file_name, parent_folder_id, file_cache=None, dry_run=False):
    """Check if a file with the same name already exists in the destination folder.
    Uses a cache to avoid repeated API calls for the same folder."""
    # Initialize the cache if not provided
    if file_cache is None:
        check_file_exists.cache = {}
    else:
        # Use the provided cache
        check_file_exists.cache = file_cache
    
    # Create a cache key based on folder ID
    cache_key = parent_folder_id
    
    # Skip API calls for non-existent folders in dry run mode
    if dry_run and parent_folder_id.startswith('dry-run-folder-'):
        if cache_key not in check_file_exists.cache:
            check_file_exists.cache[cache_key] = {}
        return None
    
    # If this folder hasn't been cached yet, fetch all files in the folder at once
    if cache_key not in check_file_exists.cache:
        try:
            # Get all files in the folder
            print_color(f"Caching files in folder {parent_folder_id}...", 'yellow')
            
            # Define a function with retry capability
            @retry_with_exponential_backoff
            def list_files_for_cache():
                return service.files().list(
                    q=f"'{parent_folder_id}' in parents and trashed=false",
                    fields="files(id, name, size)",
                    spaces='drive',
                    pageSize=1000  # Fetch more items at once
                ).execute()
            
            # Get files with retry
            response = list_files_for_cache()
            
            # Create a dictionary of filename -> file details
            files_dict = {}
            for item in response.get('files', []):
                files_dict[item['name']] = item
            
            # Store in cache
            check_file_exists.cache[cache_key] = files_dict
            print_color(f"Cached {len(files_dict)} files", 'green')
        except Exception as e:
            if not dry_run:  # Only show error in normal mode
                print_color(f"Error caching folder contents: {str(e)}", 'red')
            check_file_exists.cache[cache_key] = {}  # Set empty cache to avoid retrying
    
    # Look up the file in the cache
    return check_file_exists.cache[cache_key].get(file_name)

def retry_with_exponential_backoff(func, max_retries=8, initial_delay=10, factor=2, jitter=0.1):
    """Retry a function with exponential backoff for rate limiting issues."""
    def wrapper(*args, **kwargs):
        retry_count = 0
        delay = initial_delay
        
        while True:
            try:
                result = func(*args, **kwargs)
                # Add a small pause after successful operations to avoid hitting limits
                time.sleep(0.5)
                return result
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

def copy_file(service, file_id, name, parent_id, file_size=0, dry_run=False, file_cache=None, all_files=None):
    """Copy a file to a new location."""
    # No longer skipping macOS resource fork files
    
    # Check if file already exists in destination
    existing_file = check_file_exists(service, name, parent_id, file_cache, dry_run)
    
    # If file exists, check size
    if existing_file:
        existing_size = int(existing_file.get('size', 0))
        if existing_size == file_size and file_size > 0:
            # File exists with same size, skip
            if dry_run:
                return {"status": "skipped", "id": existing_file.get('id'), "reason": "same_size"}
            else:
                print_color(f"  File '{name}' already exists with same size, skipping", 'yellow')
                return {"status": "skipped", "id": existing_file.get('id'), "reason": "same_size"}
        else:
            # File exists but different size, overwrite
            if dry_run:
                return {"status": "would_replace", "id": existing_file.get('id'), "reason": "different_size"}
            else:
                print_color(f"  File '{name}' exists but different size, replacing", 'yellow')
                # Delete existing file
                try:
                    # Use retry for delete operation
                    @retry_with_exponential_backoff
                    def delete_file():
                        return service.files().delete(fileId=existing_file.get('id')).execute()
                    
                    delete_file()
                    
                    # Update cache - remove the deleted file
                    if file_cache is not None and parent_id in file_cache:
                        if name in file_cache[parent_id]:
                            del file_cache[parent_id][name]
                except Exception as e:
                    print_color(f"  Error deleting existing file: {str(e)}", 'red')
                    return {"status": "error", "reason": "delete_failed"}
    
    # If we're in dry run mode, just return what would happen
    if dry_run:
        return {"status": "would_copy", "name": name, "size": file_size}
    
    # Create file metadata with parent folder
    file_metadata = {
        'name': name,
        'parents': [parent_id]
    }
    
    try:
        # After a rate limit error, check if the file might have already been created
        # This handles cases where the operation succeeded but the API response failed
        double_check_existing = check_file_exists(service, name, parent_id, None, False)
        if double_check_existing:
            existing_size = int(double_check_existing.get('size', 0))
            # If a file with matching name and size exists, it was probably already copied
            if existing_size == file_size and file_size > 0:
                print_color(f"  File '{name}' appears to already exist in destination despite errors. Skipping.", 'yellow')
                # Update the cache with this file
                if file_cache is not None:
                    if parent_id not in file_cache:
                        file_cache[parent_id] = {}
                    file_cache[parent_id][name] = double_check_existing
                return {"status": "copied", "id": double_check_existing.get('id'), "reason": "already_exists"}
    
        # Apply retry with exponential backoff to the copy operation
        @retry_with_exponential_backoff
        def copy_file_with_retry():
            return service.files().copy(
                fileId=file_id,
                body=file_metadata,
                fields='id, name',
                supportsAllDrives=True
            ).execute()
        
        # Try direct copy first with retry
        file = copy_file_with_retry()
        # Clear cache for this folder since we've added a new file
        if file_cache is not None and parent_id in file_cache:
            file_cache[parent_id] = {}
        return {"status": "copied", "id": file.get('id')}
    except Exception as e:
        # First, double check again in case the file was actually copied
        # despite the error (this can happen with rate limit errors)
        double_check_existing = check_file_exists(service, name, parent_id, None, False)
        if double_check_existing:
            existing_size = int(double_check_existing.get('size', 0))
            # If a file with matching name exists, it was probably already copied
            if existing_size > 0:
                print_color(f"  File '{name}' was actually copied despite error. Size: {format_size(existing_size)}", 'green')
                # Update the cache with this file
                if file_cache is not None:
                    if parent_id not in file_cache:
                        file_cache[parent_id] = {}
                    file_cache[parent_id][name] = double_check_existing
                return {"status": "copied", "id": double_check_existing.get('id'), "reason": "already_exists"}
        
        print_color(f"Error copying file '{name}' after retries: {str(e)}", 'red')
        
        # If copy fails, try download and re-upload
        if "insufficientFilePermissions" in str(e):
            print_color(f"Permission issue detected. Trying to download and re-upload '{name}'...", 'yellow')
            try:
                # Use retry for get_media operation
                @retry_with_exponential_backoff
                def get_media_with_retry():
                    return service.files().get_media(
                        fileId=file_id,
                        supportsAllDrives=True
                    )
                
                request = get_media_with_retry()
                from io import BytesIO
                file_content = BytesIO()
                downloader = MediaIoBaseDownload(file_content, request)
                
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    print(f"Download progress: {int(status.progress() * 100)}%")
                
                file_content.seek(0)
                
                # Get file MIME type with retry
                @retry_with_exponential_backoff
                def get_file_type_with_retry():
                    return service.files().get(
                        fileId=file_id, 
                        fields='mimeType',
                        supportsAllDrives=True
                    ).execute()
                
                file_metadata_obj = get_file_type_with_retry()
                mime_type = file_metadata_obj.get('mimeType', 'application/octet-stream')
                
                media = MediaIoBaseUpload(
                    file_content, 
                    mimetype=mime_type, 
                    resumable=True
                )
                
                # Use retry for create operation
                @retry_with_exponential_backoff
                def create_file_with_retry():
                    return service.files().create(
                        body=file_metadata,
                        media_body=media,
                        fields='id, name'
                    ).execute()
                
                uploaded_file = create_file_with_retry()
                
                # Clear cache for this folder since we've added a new file
                if file_cache is not None and parent_id in file_cache:
                    file_cache[parent_id] = {}
                
                print_color(f"Successfully uploaded file '{name}' using alternative method.", 'green')
                return {"status": "uploaded", "id": uploaded_file.get('id')}
            except Exception as alt_e:
                # Check once more if the file exists
                double_check_existing = check_file_exists(service, name, parent_id, None, False)
                if double_check_existing:
                    existing_size = int(double_check_existing.get('size', 0))
                    # If a file with matching name exists, it was probably already uploaded
                    if existing_size > 0:
                        print_color(f"  File '{name}' exists despite error. Size: {format_size(existing_size)}", 'green')
                        # Update the cache with this file
                        if file_cache is not None:
                            if parent_id not in file_cache:
                                file_cache[parent_id] = {}
                            file_cache[parent_id][name] = double_check_existing
                        return {"status": "uploaded", "id": double_check_existing.get('id'), "reason": "already_exists"}
                
                print_color(f"Alternative method also failed for file '{name}': {str(alt_e)}", 'red')
                return {"status": "error", "reason": "alt_method_failed"}
        
        # Check one more time before giving up
        final_check = check_file_exists(service, name, parent_id, None, False)
        if final_check:
            print_color(f"  Despite errors, file '{name}' exists in destination. Size: {format_size(int(final_check.get('size', 0)))}", 'green')
            # Update the cache with this file
            if file_cache is not None:
                if parent_id not in file_cache:
                    file_cache[parent_id] = {}
                file_cache[parent_id][name] = final_check
            return {"status": "copied", "id": final_check.get('id'), "reason": "found_after_error"}
        
        return {"status": "error", "reason": "copy_failed"}

def copy_folder_structure(service, source_folder_id, dest_folder_id, folder_name, depth=0, dry_run=False, is_shared_drive=False, file_cache=None, operation_delay=3):
    """Copy folder structure recursively with file existence checks."""
    # Counters for summary
    summary = {
        'copied_files': 0,
        'skipped_files': 0,
        'replaced_files': 0,
        'copied_folders': 0,
        'total_copied_bytes': 0,
        'errors': 0,
        'skipped_resource_files': 0  # Track macOS resource files we skip
    }
    
    # Initialize file cache if not provided
    if file_cache is None:
        file_cache = {}
    
    # Create the folder in the destination
    indent = "  " * depth
    print(f"{indent}📁 {'[DRY RUN] Would create' if dry_run else 'Creating'} folder: {folder_name}")
    
    new_dest_folder_id = dest_folder_id
    
    # No longer skipping macOS resource files
    
    # For the root case, we either use the existing destination folder or create a new one
    if depth == 0:
        response = input(f"Create a new subfolder '{folder_name}' in the destination? (y/n): ")
        if response.lower() == 'y':
            # Check if folder already exists
            existing_folder = None
            try:
                response = service.files().list(
                    q=f"name='{folder_name}' and '{dest_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
                    fields="files(id, name)",
                    spaces='drive'
                ).execute()
                
                items = response.get('files', [])
                if items:
                    existing_folder = items[0]
            except Exception:
                pass
            
            if existing_folder:
                print_color(f"Folder '{folder_name}' already exists in destination [ID: {existing_folder.get('id')}]", 'yellow')
                confirm = input(f"Use existing folder? (y/n): ")
                if confirm.lower() == 'y':
                    new_dest_folder_id = existing_folder.get('id')
                    print_color(f"Using existing folder: {folder_name} [ID: {new_dest_folder_id}]", 'green')
                else:
                    if not dry_run:
                        new_folder = create_folder(service, folder_name, dest_folder_id)
                        if not new_folder:
                            summary['errors'] += 1
                            return summary
                        new_dest_folder_id = new_folder['id']
                        print_color(f"Created new folder: {folder_name} [ID: {new_dest_folder_id}]", 'green')
                        summary['copied_folders'] += 1
                    else:
                        print_color(f"[DRY RUN] Would create new folder: {folder_name} in destination", 'green')
                        # For dry run, assign a temporary ID for simulation
                        new_dest_folder_id = f"dry-run-folder-{depth}-{folder_name}"
                        summary['copied_folders'] += 1
            else:
                if not dry_run:
                    new_folder = create_folder(service, folder_name, dest_folder_id)
                    if not new_folder:
                        summary['errors'] += 1
                        return summary
                    new_dest_folder_id = new_folder['id']
                    print_color(f"Created new folder: {folder_name} [ID: {new_dest_folder_id}]", 'green')
                    summary['copied_folders'] += 1
                else:
                    print_color(f"[DRY RUN] Would create new folder: {folder_name} in destination", 'green')
                    # For dry run, assign a temporary ID for simulation
                    new_dest_folder_id = f"dry-run-folder-{depth}-{folder_name}"
                    summary['copied_folders'] += 1
        else:
            print_color(f"Using existing destination folder [ID: {dest_folder_id}]", 'yellow')
    else:
        # For nested folders, check if they already exist
        existing_folder = None
        try:
            response = service.files().list(
                q=f"name='{folder_name}' and '{dest_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
                fields="files(id, name)",
                spaces='drive'
            ).execute()
            
            items = response.get('files', [])
            if items:
                existing_folder = items[0]
        except Exception:
            pass
        
        if existing_folder:
            new_dest_folder_id = existing_folder.get('id')
            print_color(f"{indent}Using existing subfolder: {folder_name} [ID: {new_dest_folder_id}]", 'yellow')
        else:
            if not dry_run:
                new_folder = create_folder(service, folder_name, dest_folder_id)
                if not new_folder:
                    summary['errors'] += 1
                    return summary
                new_dest_folder_id = new_folder['id']
                summary['copied_folders'] += 1
            else:
                print_color(f"{indent}[DRY RUN] Would create subfolder: {folder_name}", 'green')
                # For dry run, assign a temporary ID for simulation
                new_dest_folder_id = f"dry-run-folder-{depth}-{folder_name}"
                summary['copied_folders'] += 1
    
    # Get all items in the source folder
    items = get_folder_contents(service, source_folder_id, is_shared_drive)
    
    # Track progress
    total_items = len(items)
    if total_items == 0:
        print(f"{indent}📂 Folder is empty")
        return summary
    
    # Count folders and files
    folder_count = sum(1 for item in items if item.get('mimeType') == 'application/vnd.google-apps.folder')
    file_count = total_items - folder_count
    print(f"{indent}Found {total_items} items: {folder_count} folders, {file_count} files")
    
    # Process all items in the folder
    for i, item in enumerate(items, 1):
        try:
            is_folder = item.get('mimeType') == 'application/vnd.google-apps.folder'
            
            # Progress indicator
            progress = f"[{i}/{total_items}]"
            
            if is_folder:
                # Recursively copy subfolders
                print(f"{indent}{progress} 📂 Processing subfolder: {item['name']}")
                subfolder_summary = copy_folder_structure(
                    service, item['id'], new_dest_folder_id, item['name'], 
                    depth + 1, dry_run, is_shared_drive, file_cache
                )
                
                # Update summary
                summary['copied_files'] += subfolder_summary['copied_files']
                summary['skipped_files'] += subfolder_summary['skipped_files']
                summary['replaced_files'] += subfolder_summary['replaced_files']
                summary['copied_folders'] += subfolder_summary['copied_folders']
                summary['total_copied_bytes'] += subfolder_summary['total_copied_bytes']
                summary['errors'] += subfolder_summary['errors']
            else:
                # Copy files with existence check
                file_size = int(item.get('size', 0))
                if dry_run:
                    result = copy_file(service, item['id'], item['name'], new_dest_folder_id, file_size, dry_run=True, file_cache=file_cache)
                    
                    if result['status'] == 'would_copy':
                        print_color(f"{indent}{progress} 📄 [DRY RUN] Would copy: {item['name']} ({format_size(file_size)})", 'green')
                        summary['copied_files'] += 1
                        summary['total_copied_bytes'] += file_size
                    elif result['status'] == 'skipped':
                        print_color(f"{indent}{progress} 📄 [DRY RUN] Would skip: {item['name']} (already exists with same size)", 'yellow')
                        summary['skipped_files'] += 1
                    elif result['status'] == 'would_replace':
                        print_color(f"{indent}{progress} 📄 [DRY RUN] Would replace: {item['name']} (exists with different size)", 'yellow')
                        summary['replaced_files'] += 1
                        summary['total_copied_bytes'] += file_size
                else:
                    print(f"{indent}{progress} 📄 Processing file: {item['name']}")
                    result = copy_file(service, item['id'], item['name'], new_dest_folder_id, file_size, file_cache=file_cache)
                    
                    if result['status'] == 'copied' or result['status'] == 'uploaded':
                        print_color(f"{indent} ✅ {'Uploaded' if result['status'] == 'uploaded' else 'Copied'}: {item['name']}", 'green')
                        summary['copied_files'] += 1
                        summary['total_copied_bytes'] += file_size
                    elif result['status'] == 'skipped':
                        print_color(f"{indent} ⏭️ Skipped: {item['name']} (already exists with same size)", 'yellow')
                        summary['skipped_files'] += 1
                    else:
                        print_color(f"{indent} ❌ Failed to copy: {item['name']} (Reason: {result.get('reason', 'unknown')})", 'red')
                        summary['errors'] += 1
                    
                    # Add a delay between file operations to avoid rate limits
                    time.sleep(operation_delay)
        
        except Exception as e:
            print_color(f"{indent}⚠️ {'[DRY RUN] ' if dry_run else ''}Error processing item {item.get('name', 'unknown')}: {str(e)}", 'red')
            summary['errors'] += 1
    
    return summary

def compare_destination(service, source_stats, dest_folder_id, dry_run=False):
    """Compare source stats with destination to identify what will be copied vs existing."""
    print_color("\nAnalyzing destination folder...", 'blue')
    
    # Initialize counters
    existing_files = 0
    existing_folders = 0
    existing_bytes = 0
    to_copy_files = 0
    to_copy_folders = 0
    to_copy_bytes = 0
    
    # Initialize file cache for faster lookups
    file_cache = {}
    
    # Set a maximum number of files to process and add progress indicators
    # This will prevent the function from taking too long
    total_files = len(source_stats['file_details'])
    print(f"Checking {total_files} files for existence in destination...")
    
    # Group files by parent folder to optimize API calls
    folder_files = {}
    for file_detail in source_stats['file_details']:
        parent_folder_id = file_detail.get('parent_folder_id')
        if parent_folder_id not in folder_files:
            folder_files[parent_folder_id] = []
        folder_files[parent_folder_id].append(file_detail)
    
    print(f"Organized files into {len(folder_files)} source folders")
    
    # Process files grouped by parent folder
    processed_files = 0
    for folder_id, files in folder_files.items():
        # Process each file from this folder
        for file_detail in files:
            file_name = file_detail['name']
            file_size = file_detail['size']
            
            # Check if similar file exists in destination folder
            existing_file = check_file_exists(service, file_name, dest_folder_id, file_cache, dry_run)
            
            if existing_file:
                existing_size = int(existing_file.get('size', 0))
                if existing_size == file_size:
                    # Already exists with same size
                    existing_files += 1
                    existing_bytes += file_size
                else:
                    # Exists but will be replaced
                    to_copy_files += 1
                    to_copy_bytes += file_size
            else:
                # Will be newly copied
                to_copy_files += 1
                to_copy_bytes += file_size
            
            # Update processed count and show progress
            processed_files += 1
            if processed_files % 50 == 0 or processed_files == total_files:
                print(f"Progress: {processed_files}/{total_files} files checked...")
    
    # Estimate folders that will be created
    to_copy_folders = source_stats['total_folders']
    
    # Display comparison results
    print_color("\n====== COPY OPERATION SUMMARY ======", 'blue')
    print(f"Files to copy:    {to_copy_files} ({format_size(to_copy_bytes)})")
    print(f"Files to skip:    {existing_files} ({format_size(existing_bytes)})")
    print(f"Folders to create: {to_copy_folders}")
    print(f"Total size on disk after copy: {format_size(to_copy_bytes + existing_bytes)}")
    
    # Calculate estimated time based on size (very rough estimate)
    estimated_minutes = to_copy_bytes / (10 * 1024 * 1024)  # Assuming 10MB/s average speed
    if estimated_minutes > 60:
        hours = estimated_minutes / 60
        print(f"Estimated time: approximately {hours:.1f} hours")
    else:
        print(f"Estimated time: approximately {estimated_minutes:.1f} minutes")
    
    print_color("==============================", 'blue')
    
    return {
        'existing_files': existing_files,
        'existing_bytes': existing_bytes,
        'to_copy_files': to_copy_files,
        'to_copy_bytes': to_copy_bytes,
        'to_copy_folders': to_copy_folders
    }

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Copy Google Drive folders while preserving structure.')
    parser.add_argument('--dry-run', action='store_true', help='Simulate the copy process without making any changes')
    parser.add_argument('--source', default=SOURCE_FOLDER_ID, help=f'Source folder ID (default: {SOURCE_FOLDER_ID})')
    parser.add_argument('--dest', default=DEFAULT_DESTINATION_ID, help=f'Destination folder ID (default: {DEFAULT_DESTINATION_ID})')
    
    # Add rate limiting options
    parser.add_argument('--max-retries', type=int, default=8, help='Maximum number of retry attempts for rate limited operations (default: 8)')
    parser.add_argument('--initial-delay', type=float, default=10, help='Initial delay in seconds before first retry (default: 10)')
    parser.add_argument('--backoff-factor', type=float, default=2, help='Factor by which the delay increases with each retry (default: 2)')
    parser.add_argument('--operation-delay', type=float, default=3, help='Delay in seconds between file operations (default: 3)')
    
    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_args()
    dry_run = args.dry_run
    source_id = None  # Will be selected interactively
    dest_default_id = args.dest
    
    # Set up the retry function parameters
    global retry_with_exponential_backoff
    # Save the original function
    original_retry_function = retry_with_exponential_backoff
    # Replace it with a version using user-specified parameters
    def custom_retry_function(func):
        return original_retry_function(
            func, 
            max_retries=args.max_retries,
            initial_delay=args.initial_delay,
            factor=args.backoff_factor
        )
    retry_with_exponential_backoff = custom_retry_function
    
    if dry_run:
        print("====== GOOGLE DRIVE FOLDER COPY [DRY RUN MODE] ======")
        print("This tool will simulate copying the folder structure without making any changes.")
    else:
        print("====== GOOGLE DRIVE DIRECT FOLDER COPY ======")
        print("This tool will copy folders from Google Drive to your Drive.")
    
    if dry_run:
        print("No files or folders will be created - this is just a simulation.")
    
    try:
        # Get credentials and build service
        creds = get_credentials()
        service = build('drive', 'v3', credentials=creds)
        print_color("✅ Successfully connected to Google Drive", 'green')
        
        # List shared folders and let user select one
        source_folder = list_shared_folders(service)
        if not source_folder:
            print_color("❌ No source folder selected. Exiting.", 'red')
            return 1
        
        # Set the selected source ID
        source_id = source_folder.get('id')
        source_name = source_folder.get('name')
        
        print_color(f"\n📁 Selected source folder: {source_name} [ID: {source_id}]", 'blue')
        
        # Check if it's a shared drive
        is_shared_drive = source_folder.get('isSharedDrive', False)
        
        # Start scanning folder recursively to get stats
        print_color("\nScanning folder structure and calculating sizes...", 'blue')
        print("This may take a moment depending on the folder size and depth...")
        
        # Scan recursively and get stats
        stats = scan_folder_recursive(service, source_id, source_name, is_shared_drive)
        
        # Display folder statistics
        display_folder_stats(stats)
        
        # Check if we found anything
        if stats['total_folders'] == 0 and stats['total_files'] == 0:
            print_color("No items found in source folder (empty or no access to contents)", 'yellow')
            return 1
        
        # Select destination folder
        if not dry_run or dry_run:  # Always select destination even in dry run mode
            dest_folder = list_destination_folders(service)
            if not dest_folder:
                print_color("❌ No destination folder selected. Exiting.", 'red')
                return 1
            
            # For dry run with specific dest parameter
            if dest_folder.get('id') != dest_default_id and args.dest == DEFAULT_DESTINATION_ID:
                dest_default_id = dest_folder.get('id')
        
        # Compare source with destination to provide a summary before copying
        comparison = compare_destination(service, stats, dest_folder.get('id'), dry_run)
        
        # Confirm and start copying
        if dry_run:
            confirm_msg = f"\nDo you want to simulate copying '{source_name}' to '{dest_folder.get('name')}'? (y/n): "
        else:
            confirm_msg = f"\nDo you want to copy '{source_name}' to '{dest_folder.get('name')}'? (y/n): "
            
        confirm = input(confirm_msg)
        if confirm.lower() != 'y':
            print_color("Operation cancelled.", 'yellow')
            return 0
        
        print_color("\nStarting " + ("simulation..." if dry_run else "copy process..."), 'blue')
        
        # Start timer
        start_time = time.time()
        
        # Initialize a file cache for the entire operation
        file_cache = {}
        
        # Copy the folder structure with operation delay
        print_color(f"\nUsing rate limit parameters:", 'blue')
        print(f"- Initial delay: {args.initial_delay} seconds")
        print(f"- Backoff factor: {args.backoff_factor}x")
        print(f"- Maximum retries: {args.max_retries}")
        print(f"- Delay between operations: {args.operation_delay} seconds")
        
        summary = copy_folder_structure(
            service, 
            source_id, 
            dest_folder.get('id'), 
            source_name, 
            dry_run=dry_run, 
            is_shared_drive=is_shared_drive, 
            file_cache=file_cache,
            operation_delay=args.operation_delay
        )
        
        # End timer
        elapsed_time = time.time() - start_time
        minutes, seconds = divmod(elapsed_time, 60)
        hours, minutes = divmod(minutes, 60)
        
        # Display summary results
        if dry_run:
            print("\n====== SIMULATION COMPLETED ======")
        else:
            print("\n====== COPY PROCESS COMPLETED ======")
            
        print(f"Time elapsed: {int(hours)}h {int(minutes)}m {int(seconds)}s")
        print(f"\nSource folder: {source_name} [ID: {source_id}]")
        print(f"Destination folder: {dest_folder.get('name')} [ID: {dest_folder.get('id')}]")
        
        print_color("\n===== OPERATION SUMMARY =====", 'blue')
        print(f"Folders created: {summary['copied_folders']}")
        print(f"Files copied:    {summary['copied_files']} ({format_size(summary['total_copied_bytes'])})")
        print(f"Files skipped:   {summary['skipped_files']} (already exist with same size)")
        print(f"Files replaced:  {summary['replaced_files']} (existed with different size)")
        
        # No longer skipping macOS resource files
        
        if summary['errors'] > 0:
            print_color(f"Errors:         {summary['errors']}", 'red')
            print_color("\nSome files or folders were not copied due to errors.", 'red')
            print("Check the output above for specific error messages.")
        
        if dry_run:
            print("\nTo perform the actual copy operation, run the script without the --dry-run flag.")
        
    except Exception as e:
        print_color(f"\n❌ Error: {str(e)}", 'red')
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())