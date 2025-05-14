#!/usr/bin/env python3

import os
import pickle
import sys
import time
import argparse
import random
from collections import defaultdict
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
                    fields='nextPageToken, files(id, name, mimeType, size, md5Checksum)',
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
                    fields='nextPageToken, files(id, name, mimeType, size, md5Checksum)',
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
            'files_by_extension': defaultdict(int),
            'file_index': {},  # Will store {path: {name, size, id, md5}}
            'folder_sizes': {},  # Will track folder sizes {folder_path: size}
            'folder_paths': {},  # Will store folder paths {folder_id: path}
        }
    
    # Get all folder contents
    items = get_folder_contents(service, folder_id, is_shared_drive)
    
    # Process folders first
    folders = [item for item in items if item.get('mimeType') == 'application/vnd.google-apps.folder']
    files = [item for item in items if item.get('mimeType') != 'application/vnd.google-apps.folder']
    
    # Update folder count
    stats['total_folders'] += len(folders)
    stats['total_files'] += len(files)
    
    # Store folder path
    stats['folder_paths'][folder_id] = folder_name
    
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
        stats['files_by_extension'][ext] += 1
        
        # Get file path
        file_path = f"{folder_name}/{file_name}"
        
        # Store file details
        stats['file_index'][file_path] = {
            'id': file.get('id'),
            'name': file_name,
            'size': file_size,
            'md5': file.get('md5Checksum', ''),
            'parent_folder': folder_name
        }
    
    # Store folder size
    stats['folder_sizes'][folder_name] = folder_size
    
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
    if size_bytes == 0:
        return "0 B"
    elif size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

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
            folder_sources.append('My Drive')
        
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

def compare_folders(stats1, stats2):
    """Compare two folder statistics and generate a diff report."""
    diff = {
        # Summary statistics
        'total_files_diff': stats2['total_files'] - stats1['total_files'],
        'total_folders_diff': stats2['total_folders'] - stats1['total_folders'],
        'total_size_diff': stats2['total_size_bytes'] - stats1['total_size_bytes'],
        
        # Detailed differences
        'files_only_in_first': [],    # Files in stats1 but not in stats2
        'files_only_in_second': [],   # Files in stats2 but not in stats1
        'different_files': [],        # Files that exist in both but have different sizes/MD5s
        'matching_files': [],         # Files that are identical in both folders
        
        # File type differences
        'extensions_diff': {},        # Difference in file counts by extension
        
        # Size analysis
        'largest_missing_files': [],  # Largest files missing from second folder
        'largest_extra_files': [],    # Largest files only in second folder
        'largest_different_files': [] # Largest files that are different
    }
    
    # Compare files
    all_files = set(stats1['file_index'].keys()) | set(stats2['file_index'].keys())
    
    for file_path in all_files:
        if file_path in stats1['file_index'] and file_path not in stats2['file_index']:
            # File only in first folder
            file_details = stats1['file_index'][file_path]
            diff['files_only_in_first'].append({
                'path': file_path,
                'size': file_details['size'],
                'name': file_details['name']
            })
            
        elif file_path in stats2['file_index'] and file_path not in stats1['file_index']:
            # File only in second folder
            file_details = stats2['file_index'][file_path]
            diff['files_only_in_second'].append({
                'path': file_path,
                'size': file_details['size'],
                'name': file_details['name']
            })
            
        else:
            # File in both folders, check if they're different
            file1 = stats1['file_index'][file_path]
            file2 = stats2['file_index'][file_path]
            
            if file1['size'] != file2['size'] or (file1['md5'] and file2['md5'] and file1['md5'] != file2['md5']):
                # Files are different
                diff['different_files'].append({
                    'path': file_path,
                    'name': file1['name'],
                    'size_diff': file2['size'] - file1['size'],
                    'size1': file1['size'],
                    'size2': file2['size']
                })
            else:
                # Files are the same
                diff['matching_files'].append({
                    'path': file_path,
                    'size': file1['size'],
                    'name': file1['name']
                })
    
    # Compare file extensions
    all_extensions = set(stats1['files_by_extension'].keys()) | set(stats2['files_by_extension'].keys())
    for ext in all_extensions:
        count1 = stats1['files_by_extension'].get(ext, 0)
        count2 = stats2['files_by_extension'].get(ext, 0)
        diff['extensions_diff'][ext] = count2 - count1
    
    # Sort by size for the largest files lists
    diff['files_only_in_first'] = sorted(diff['files_only_in_first'], key=lambda x: x['size'], reverse=True)
    diff['files_only_in_second'] = sorted(diff['files_only_in_second'], key=lambda x: x['size'], reverse=True)
    diff['different_files'] = sorted(diff['different_files'], key=lambda x: abs(x['size_diff']), reverse=True)
    
    # Extract the largest files for each category
    diff['largest_missing_files'] = diff['files_only_in_first'][:10]  # Top 10
    diff['largest_extra_files'] = diff['files_only_in_second'][:10]   # Top 10
    diff['largest_different_files'] = diff['different_files'][:10]    # Top 10
    
    return diff

def display_diff(folder1_name, folder2_name, diff):
    """Display folder comparison results in a visually appealing way."""
    # Function to draw a separator line
    def separator(char='-', length=80):
        return char * length
    
    # Summary header
    print_color("\n" + separator("="), 'blue')
    print_color(f" FOLDER COMPARISON: '{folder1_name}' vs '{folder2_name}' ", 'bold')
    print_color(separator("=") + "\n", 'blue')
    
    # Overall statistics
    print_color("📊 SUMMARY", 'bold')
    
    # Format the difference arrows
    def diff_arrow(value):
        if value > 0:
            return f"↑ +{value}"
        elif value < 0:
            return f"↓ {value}"
        else:
            return "="
    
    # Calculate match percentage
    total_unique_files = len(diff['files_only_in_first']) + len(diff['files_only_in_second']) + len(diff['different_files']) + len(diff['matching_files'])
    match_percentage = (len(diff['matching_files']) / total_unique_files * 100) if total_unique_files > 0 else 0
    
    # Summary table
    print("")
    print(f"{'CATEGORY':<20} {'FOLDER 1':<15} {'FOLDER 2':<15} {'DIFFERENCE':<15}")
    print(separator("-"))
    
    # Files
    files_diff = diff_arrow(diff['total_files_diff'])
    print(f"{'Files':<20} {len(diff['matching_files']) + len(diff['files_only_in_first']) + len(diff['different_files']):<15} {len(diff['matching_files']) + len(diff['files_only_in_second']) + len(diff['different_files']):<15} {files_diff:<15}")
    
    # Folders
    folders_diff = diff_arrow(diff['total_folders_diff'])
    print(f"{'Folders':<20} {diff['total_folders_diff']:<15} {diff['total_folders_diff']:<15} {folders_diff:<15}")
    
    # Total size
    total_size1 = sum(file['size'] for file in diff['matching_files']) + sum(file['size'] for file in diff['files_only_in_first']) + sum(file['size1'] for file in diff['different_files'])
    total_size2 = sum(file['size'] for file in diff['matching_files']) + sum(file['size'] for file in diff['files_only_in_second']) + sum(file['size2'] for file in diff['different_files'])
    size_diff = total_size2 - total_size1
    size_diff_str = diff_arrow(size_diff)
    
    print(f"{'Total Size':<20} {format_size(total_size1):<15} {format_size(total_size2):<15} {format_size(size_diff) + ' ' + size_diff_str:<15}")
    
    # Match percentage
    print(f"{'Match Percentage':<20} {'':<15} {'':<15} {match_percentage:.1f}%")
    
    # Content Differences
    print_color("\n📋 CONTENT DIFFERENCES", 'bold')
    
    # Only in first
    missing_count = len(diff['files_only_in_first'])
    missing_size = sum(file['size'] for file in diff['files_only_in_first'])
    print_color(f"\n🟥 Files only in '{folder1_name}': {missing_count} files ({format_size(missing_size)})", 'red')
    
    # Show the largest missing files
    if diff['largest_missing_files']:
        print("   Largest missing files:")
        for i, file in enumerate(diff['largest_missing_files'][:5], 1):  # Show top 5
            print(f"   {i}. {file['name']} ({format_size(file['size'])})")
    
    # Only in second
    extra_count = len(diff['files_only_in_second'])
    extra_size = sum(file['size'] for file in diff['files_only_in_second'])
    print_color(f"\n🟩 Files only in '{folder2_name}': {extra_count} files ({format_size(extra_size)})", 'green')
    
    # Show the largest extra files
    if diff['largest_extra_files']:
        print("   Largest extra files:")
        for i, file in enumerate(diff['largest_extra_files'][:5], 1):  # Show top 5
            print(f"   {i}. {file['name']} ({format_size(file['size'])})")
    
    # Different files
    diff_count = len(diff['different_files'])
    diff_size1 = sum(file['size1'] for file in diff['different_files'])
    diff_size2 = sum(file['size2'] for file in diff['different_files'])
    diff_size_change = diff_size2 - diff_size1
    diff_size_change_str = "larger" if diff_size_change > 0 else "smaller"
    
    print_color(f"\n🟨 Files with differences: {diff_count} files", 'yellow')
    if diff_count > 0:
        print(f"   Files in '{folder2_name}' are {format_size(abs(diff_size_change))} {diff_size_change_str} than in '{folder1_name}'")
    
    # Show the largest different files
    if diff['largest_different_files']:
        print("   Largest differences:")
        for i, file in enumerate(diff['largest_different_files'][:5], 1):  # Show top 5
            change = "larger" if file['size_diff'] > 0 else "smaller"
            print(f"   {i}. {file['name']} ({format_size(abs(file['size_diff']))} {change})")
    
    # Matching files
    match_count = len(diff['matching_files'])
    match_size = sum(file['size'] for file in diff['matching_files'])
    print_color(f"\n🟦 Identical files: {match_count} files ({format_size(match_size)})", 'blue')
    
    # File Types
    print_color("\n📁 FILE TYPE DIFFERENCES", 'bold')
    
    # Only show non-zero differences
    ext_diffs = {ext: count for ext, count in diff['extensions_diff'].items() if count != 0}
    sorted_exts = sorted(ext_diffs.items(), key=lambda x: abs(x[1]), reverse=True)
    
    if sorted_exts:
        print("\n   Type      Difference")
        print("   " + separator("-", 25))
        
        for ext, count_diff in sorted_exts[:10]:  # Show top 10 differences
            diff_str = f"+{count_diff}" if count_diff > 0 else f"{count_diff}"
            color = 'green' if count_diff > 0 else 'red'
            print_color(f"   .{ext:<8} {diff_str:>10}", color)
    else:
        print("\n   No differences in file types")
    
    # Final summary
    print_color("\n" + separator("="), 'blue')
    
    # Overall assessment
    if missing_count == 0 and extra_count == 0 and diff_count == 0:
        print_color("✅ The folders are identical!", 'green')
    elif match_percentage > 90:
        print_color(f"✅ The folders are {match_percentage:.1f}% identical with minor differences.", 'green')
    elif match_percentage > 50:
        print_color(f"⚠️ The folders have significant differences ({match_percentage:.1f}% match).", 'yellow')
    else:
        print_color(f"❌ The folders are substantially different ({match_percentage:.1f}% match).", 'red')
    
    print_color(separator("=") + "\n", 'blue')

def main():
    print_color("=== GOOGLE DRIVE FOLDER COMPARISON ===", 'blue')
    print("This tool compares two Google Drive folders and analyzes their differences.")
    
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
        
        # Let user select the first folder
        print_color("\nSelect the FIRST folder to compare:", 'bold')
        
        while True:
            try:
                choice1 = input("Enter folder number (or 'q' to quit): ")
                
                if choice1.lower() == 'q':
                    print_color("Exiting...", 'yellow')
                    return 0
                
                index1 = int(choice1) - 1
                if 0 <= index1 < len(all_folders):
                    selected_folder1 = all_folders[index1]
                    break
                else:
                    print_color("Invalid selection. Please try again.", 'red')
            except ValueError:
                print_color("Please enter a number or 'q'.", 'red')
        
        # Let user select the second folder
        print_color("\nSelect the SECOND folder to compare:", 'bold')
        
        while True:
            try:
                choice2 = input("Enter folder number (or 'q' to quit): ")
                
                if choice2.lower() == 'q':
                    print_color("Exiting...", 'yellow')
                    return 0
                
                index2 = int(choice2) - 1
                if 0 <= index2 < len(all_folders):
                    selected_folder2 = all_folders[index2]
                    break
                else:
                    print_color("Invalid selection. Please try again.", 'red')
            except ValueError:
                print_color("Please enter a number or 'q'.", 'red')
        
        # Get folder details
        folder1_id = selected_folder1.get('id')
        folder1_name = selected_folder1.get('name')
        is_shared_drive1 = selected_folder1.get('isSharedDrive', False)
        
        folder2_id = selected_folder2.get('id')
        folder2_name = selected_folder2.get('name')
        is_shared_drive2 = selected_folder2.get('isSharedDrive', False)
        
        print_color(f"\n📁 Selected folders to compare:", 'blue')
        print_color(f"   1. {folder1_name} [ID: {folder1_id}]", 'cyan')
        print_color(f"   2. {folder2_name} [ID: {folder2_id}]", 'cyan')
        
        # Start scanning first folder
        print_color(f"\nScanning first folder: '{folder1_name}'...", 'blue')
        print("This may take a moment depending on the folder size and depth...")
        
        # Scan recursively and get stats for first folder
        stats1 = scan_folder_recursive(service, folder1_id, folder1_name, is_shared_drive1)
        
        # Show simple stats for first folder
        print_color(f"✅ Scan complete: '{folder1_name}' contains {stats1['total_files']} files, {stats1['total_folders']} folders ({format_size(stats1['total_size_bytes'])})", 'green')
        
        # Start scanning second folder
        print_color(f"\nScanning second folder: '{folder2_name}'...", 'blue')
        print("This may take a moment depending on the folder size and depth...")
        
        # Scan recursively and get stats for second folder
        stats2 = scan_folder_recursive(service, folder2_id, folder2_name, is_shared_drive2)
        
        # Show simple stats for second folder
        print_color(f"✅ Scan complete: '{folder2_name}' contains {stats2['total_files']} files, {stats2['total_folders']} folders ({format_size(stats2['total_size_bytes'])})", 'green')
        
        # Compare the folders
        print_color("\nComparing folders...", 'blue')
        diff = compare_folders(stats1, stats2)
        
        # Display the comparison results
        display_diff(folder1_name, folder2_name, diff)
        
    except Exception as e:
        print_color(f"\n❌ Error: {str(e)}", 'red')
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())