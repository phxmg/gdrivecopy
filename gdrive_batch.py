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

def retry_with_exponential_backoff(func, max_retries=5, initial_delay=10, factor=2, jitter=0.2):
    """Retry a function with exponential backoff for rate limiting issues."""
    def wrapper(*args, **kwargs):
        retry_count = 0
        delay = initial_delay
        
        while True:
            try:
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
                    delay = min(delay * factor, 600)  # Cap at 10 minutes
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

def create_folder(service, name, parent_id):
    """Create a new folder."""
    file_metadata = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_id]
    }
    
    try:
        @retry_with_exponential_backoff
        def create_folder_with_retry():
            return service.files().create(
                body=file_metadata, 
                fields='id, name'
            ).execute()
        
        folder = create_folder_with_retry()
        return folder
    except Exception as e:
        print_color(f"Error creating folder '{name}': {str(e)}", 'red')
        return None

def check_file_exists(service, file_name, parent_folder_id, file_cache=None):
    """Check if a file with the same name already exists in the destination folder."""
    # Initialize the cache if not provided
    if file_cache is None:
        check_file_exists.cache = {}
    else:
        # Use the provided cache
        check_file_exists.cache = file_cache
    
    # Create a cache key based on folder ID
    cache_key = parent_folder_id
    
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
            print_color(f"Error caching folder contents: {str(e)}", 'red')
            check_file_exists.cache[cache_key] = {}  # Set empty cache to avoid retrying
    
    # Look up the file in the cache
    return check_file_exists.cache[cache_key].get(file_name)

def copy_file(service, file_id, name, parent_id, file_size=0, file_cache=None, batch_delay=0):
    """Copy a file to a new location."""
    # Check if file already exists in destination
    existing_file = check_file_exists(service, name, parent_id, file_cache)
    
    # If file exists, check size
    if existing_file:
        existing_size = int(existing_file.get('size', 0))
        if existing_size == file_size and file_size > 0:
            # File exists with same size, skip
            print_color(f"  File '{name}' already exists with same size, skipping", 'yellow')
            return {"status": "skipped", "id": existing_file.get('id'), "reason": "same_size"}
        else:
            # File exists but different size, overwrite
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
    
    # Create file metadata with parent folder
    file_metadata = {
        'name': name,
        'parents': [parent_id]
    }
    
    try:
        # After a rate limit error, check if the file might have already been created
        # This handles cases where the operation succeeded but the API response failed
        double_check_existing = check_file_exists(service, name, parent_id, None)
        if double_check_existing:
            existing_size = int(double_check_existing.get('size', 0))
            # If a file with matching name and size exists, it was probably already copied
            if existing_size == file_size and file_size > 0:
                print_color(f"  File '{name}' appears to already exist in destination. Skipping.", 'yellow')
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
        
        # If batch_delay is specified, sleep between operations
        if batch_delay > 0:
            time.sleep(batch_delay)
        
        # Clear cache for this folder since we've added a new file
        if file_cache is not None and parent_id in file_cache:
            file_cache[parent_id] = {}
        return {"status": "copied", "id": file.get('id')}
    except Exception as e:
        # First, double check again in case the file was actually copied
        # despite the error (this can happen with rate limit errors)
        double_check_existing = check_file_exists(service, name, parent_id, None)
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
                
                # If batch_delay is specified, sleep between operations
                if batch_delay > 0:
                    time.sleep(batch_delay)
                
                # Clear cache for this folder since we've added a new file
                if file_cache is not None and parent_id in file_cache:
                    file_cache[parent_id] = {}
                
                print_color(f"Successfully uploaded file '{name}' using alternative method.", 'green')
                return {"status": "uploaded", "id": uploaded_file.get('id')}
            except Exception as alt_e:
                # Check once more if the file exists
                double_check_existing = check_file_exists(service, name, parent_id, None)
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
        final_check = check_file_exists(service, name, parent_id, None)
        if final_check:
            print_color(f"  Despite errors, file '{name}' exists in destination. Size: {format_size(int(final_check.get('size', 0)))}", 'green')
            # Update the cache with this file
            if file_cache is not None:
                if parent_id not in file_cache:
                    file_cache[parent_id] = {}
                file_cache[parent_id][name] = final_check
            return {"status": "copied", "id": final_check.get('id'), "reason": "found_after_error"}
        
        return {"status": "error", "reason": "copy_failed"}

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

def process_folder_recursive(service, source_folder_id, dest_folder_id, folder_name, depth=0, 
                           is_shared_drive=False, file_cache=None, batch_size=5, batch_delay=10, 
                           file_delay=2, max_items=None, progress_path=None, resume=False):
    """
    Copy folder structure recursively with careful batch processing to avoid rate limits.
    
    Parameters:
    - batch_size: Number of files to process before taking a longer pause
    - batch_delay: Seconds to wait between batches
    - file_delay: Seconds to wait between individual file operations
    - max_items: Maximum number of items (files+folders) to process before stopping
    - progress_path: Path to save progress file
    - resume: Whether to resume from last saved progress
    """
    # Initialize counters
    summary = {
        'copied_files': 0,
        'skipped_files': 0,
        'replaced_files': 0,
        'copied_folders': 0,
        'total_copied_bytes': 0,
        'errors': 0,
        'total_items_processed': 0,
        'remaining_items': max_items
    }
    
    # Initialize file cache if not provided
    if file_cache is None:
        file_cache = {}
    
    # Check if we've hit our item limit
    if max_items is not None and max_items <= 0:
        print_color("Maximum item limit reached. Stopping processing.", 'yellow')
        return summary
    
    # Create the folder in the destination
    indent = "  " * depth
    print(f"{indent}📁 Creating folder: {folder_name}")
    
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
                    new_folder = create_folder(service, folder_name, dest_folder_id)
                    if not new_folder:
                        summary['errors'] += 1
                        return summary
                    new_dest_folder_id = new_folder['id']
                    print_color(f"Created new folder: {folder_name} [ID: {new_dest_folder_id}]", 'green')
                    summary['copied_folders'] += 1
            else:
                new_folder = create_folder(service, folder_name, dest_folder_id)
                if not new_folder:
                    summary['errors'] += 1
                    return summary
                new_dest_folder_id = new_folder['id']
                print_color(f"Created new folder: {folder_name} [ID: {new_dest_folder_id}]", 'green')
                summary['copied_folders'] += 1
        else:
            print_color(f"Using existing destination folder [ID: {dest_folder_id}]", 'yellow')
            new_dest_folder_id = dest_folder_id
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
            new_folder = create_folder(service, folder_name, dest_folder_id)
            if not new_folder:
                summary['errors'] += 1
                return summary
            new_dest_folder_id = new_folder['id']
            summary['copied_folders'] += 1
            
            # Short delay after creating folder
            time.sleep(file_delay)
    
    # Get all items in the source folder
    print_color(f"{indent}Getting contents of folder {folder_name}...", 'blue')
    items = get_folder_contents(service, source_folder_id, is_shared_drive)
    
    # Check for empty folder
    if not items:
        print_color(f"{indent}📂 Folder is empty", 'yellow')
        return summary
    
    # Get folder and file counts
    folders = [item for item in items if item.get('mimeType') == 'application/vnd.google-apps.folder']
    files = [item for item in items if item.get('mimeType') != 'application/vnd.google-apps.folder']
    
    print_color(f"{indent}Found {len(items)} items: {len(folders)} folders, {len(files)} files", 'blue')
    
    # Load progress if resuming
    processed_items = {}
    if resume and progress_path and os.path.exists(progress_path):
        try:
            with open(progress_path, 'r') as f:
                for line in f:
                    item_id, status = line.strip().split(',')
                    processed_items[item_id] = status
            print_color(f"Resuming from previous progress. {len(processed_items)} items already processed.", 'green')
        except Exception as e:
            print_color(f"Error loading progress file: {str(e)}", 'red')
    
    # Process folders first (sequential processing)
    for i, folder in enumerate(folders, 1):
        folder_id = folder.get('id')
        folder_name_sub = folder.get('name')
        
        # Skip if already processed
        if folder_id in processed_items:
            print_color(f"{indent}Skipping already processed folder: {folder_name_sub}", 'yellow')
            continue
        
        # Check if we've hit our item limit
        if max_items is not None:
            summary['remaining_items'] = max_items - summary['total_items_processed']
            if summary['remaining_items'] <= 0:
                print_color(f"{indent}Maximum item limit reached. Stopping processing.", 'yellow')
                break
        
        # Process folder recursively
        print_color(f"{indent}[{i}/{len(folders)}] 📂 Processing subfolder: {folder_name_sub}", 'blue')
        subfolder_summary = process_folder_recursive(
            service, folder_id, new_dest_folder_id, folder_name_sub,
            depth + 1, is_shared_drive, file_cache, 
            batch_size, batch_delay, file_delay,
            summary['remaining_items'], progress_path, resume
        )
        
        # Update summary
        summary['copied_files'] += subfolder_summary['copied_files']
        summary['skipped_files'] += subfolder_summary['skipped_files']
        summary['replaced_files'] += subfolder_summary['replaced_files']
        summary['copied_folders'] += subfolder_summary['copied_folders']
        summary['total_copied_bytes'] += subfolder_summary['total_copied_bytes']
        summary['errors'] += subfolder_summary['errors']
        summary['total_items_processed'] += 1 + subfolder_summary['total_items_processed']
        
        # Save progress
        if progress_path:
            with open(progress_path, 'a') as f:
                f.write(f"{folder_id},folder\n")
        
        # Short delay between folders
        time.sleep(file_delay)
    
    # Process files in batches
    current_batch = 0
    batch_count = (len(files) + batch_size - 1) // batch_size  # Ceiling division
    
    for i in range(0, len(files), batch_size):
        current_batch += 1
        batch_files = files[i:i+batch_size]
        print_color(f"{indent}Processing file batch {current_batch}/{batch_count} ({len(batch_files)} files)...", 'blue')
        
        # Process each file in the batch
        for j, file_item in enumerate(batch_files, 1):
            file_id = file_item.get('id')
            file_name = file_item.get('name')
            file_size = int(file_item.get('size', 0))
            
            # Skip if already processed
            if file_id in processed_items:
                print_color(f"{indent}  Skipping already processed file: {file_name}", 'yellow')
                continue
            
            # Check if we've hit our item limit
            if max_items is not None:
                summary['remaining_items'] = max_items - summary['total_items_processed']
                if summary['remaining_items'] <= 0:
                    print_color(f"{indent}Maximum item limit reached. Stopping processing.", 'yellow')
                    break
            
            # Process file
            print_color(f"{indent}  [{j}/{len(batch_files)}] Processing file: {file_name} ({format_size(file_size)})", 'blue')
            result = copy_file(service, file_id, file_name, new_dest_folder_id, file_size, file_cache, file_delay)
            
            # Update summary based on result
            if result['status'] == 'copied' or result['status'] == 'uploaded':
                print_color(f"{indent}    ✅ {'Uploaded' if result['status'] == 'uploaded' else 'Copied'}: {file_name}", 'green')
                summary['copied_files'] += 1
                summary['total_copied_bytes'] += file_size
            elif result['status'] == 'skipped':
                print_color(f"{indent}    ⏭️ Skipped: {file_name} (already exists with same size)", 'yellow')
                summary['skipped_files'] += 1
            else:
                print_color(f"{indent}    ❌ Failed to copy: {file_name} (Reason: {result.get('reason', 'unknown')})", 'red')
                summary['errors'] += 1
            
            # Update processed items count
            summary['total_items_processed'] += 1
            
            # Save progress
            if progress_path:
                with open(progress_path, 'a') as f:
                    f.write(f"{file_id},{result['status']}\n")
        
        # Wait between batches (but not after the last batch)
        if current_batch < batch_count:
            print_color(f"{indent}Batch {current_batch} complete. Taking a break for {batch_delay} seconds to avoid rate limits...", 'yellow')
            time.sleep(batch_delay)
        else:
            print_color(f"{indent}Final batch complete.", 'green')
    
    return summary

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Batch copy Google Drive folders while avoiding rate limits')
    parser.add_argument('--source', help='Source folder ID')
    parser.add_argument('--dest', help='Destination folder ID')
    
    # Add batch processing options
    parser.add_argument('--batch-size', type=int, default=5, 
                       help='Number of files to process before taking a longer pause (default: 5)')
    parser.add_argument('--batch-delay', type=int, default=30,
                       help='Seconds to wait between batches (default: 30)')
    parser.add_argument('--file-delay', type=int, default=3,
                       help='Seconds to wait between individual file operations (default: 3)')
    parser.add_argument('--max-items', type=int, 
                       help='Maximum number of items to process before stopping')
    
    # Add retry options
    parser.add_argument('--max-retries', type=int, default=5,
                       help='Maximum number of retry attempts for rate limited operations (default: 5)')
    parser.add_argument('--initial-delay', type=int, default=10,
                       help='Initial delay in seconds before first retry (default: 10)')
    parser.add_argument('--backoff-factor', type=float, default=2.0,
                       help='Factor by which the delay increases with each retry (default: 2.0)')
    
    # Add progress tracking
    parser.add_argument('--progress-file', 
                       help='File to save progress for resuming later')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from last saved progress')
    
    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_args()
    
    # Set up retry function parameters
    global retry_with_exponential_backoff
    original_retry_function = retry_with_exponential_backoff
    
    def custom_retry_function(func):
        return original_retry_function(
            func, 
            max_retries=args.max_retries,
            initial_delay=args.initial_delay,
            factor=args.backoff_factor
        )
    
    retry_with_exponential_backoff = custom_retry_function
    
    print_color("=== GOOGLE DRIVE BATCH FOLDER COPY ===", 'blue')
    print("This tool copies folders from Google Drive to your Drive with rate limiting safeguards.")
    print_color(f"Batch processing: {args.batch_size} files per batch, {args.batch_delay}s between batches", 'cyan')
    
    try:
        # Get credentials and build service
        creds = get_credentials()
        service = build('drive', 'v3', credentials=creds)
        print_color("✅ Successfully connected to Google Drive", 'green')
        
        # Get source folder ID
        source_id = args.source
        
        if not source_id:
            print_color("\nNo source ID provided. Please select a folder:", 'yellow')
            
            # List Google Drive root folders
            response = service.files().list(
                q="mimeType='application/vnd.google-apps.folder'",
                spaces='drive',
                fields='files(id, name)',
                pageSize=20
            ).execute()
            
            folders = response.get('files', [])
            
            if not folders:
                print_color("No folders found in your Google Drive.", 'red')
                return 1
            
            print_color("Available folders:", 'blue')
            for i, folder in enumerate(folders, 1):
                print(f"{i}. {folder.get('name')} [ID: {folder.get('id')}]")
            
            choice = input("\nSelect source folder number: ")
            try:
                index = int(choice) - 1
                if 0 <= index < len(folders):
                    source_id = folders[index].get('id')
                    source_name = folders[index].get('name')
                else:
                    print_color("Invalid selection.", 'red')
                    return 1
            except ValueError:
                print_color("Please enter a number.", 'red')
                return 1
        else:
            # Get folder name
            try:
                folder = service.files().get(
                    fileId=source_id,
                    fields='name',
                    supportsAllDrives=True
                ).execute()
                source_name = folder.get('name')
            except Exception as e:
                print_color(f"Error getting source folder: {str(e)}", 'red')
                return 1
        
        print_color(f"\nUsing source folder: {source_name} [ID: {source_id}]", 'green')
        
        # Get destination folder ID
        dest_id = args.dest
        
        if not dest_id:
            print_color("\nNo destination ID provided. Please select a folder:", 'yellow')
            
            # List user's My Drive folders
            response = service.files().list(
                q="mimeType='application/vnd.google-apps.folder' and 'root' in parents",
                spaces='drive',
                fields='files(id, name)',
                pageSize=20
            ).execute()
            
            folders = response.get('files', [])
            
            if not folders:
                print_color("No folders found in your My Drive.", 'red')
                return 1
            
            print_color("Available folders in My Drive:", 'blue')
            for i, folder in enumerate(folders, 1):
                print(f"{i}. {folder.get('name')} [ID: {folder.get('id')}]")
            
            choice = input("\nSelect destination folder number: ")
            try:
                index = int(choice) - 1
                if 0 <= index < len(folders):
                    dest_id = folders[index].get('id')
                    dest_name = folders[index].get('name')
                else:
                    print_color("Invalid selection.", 'red')
                    return 1
            except ValueError:
                print_color("Please enter a number.", 'red')
                return 1
        else:
            # Get folder name
            try:
                folder = service.files().get(
                    fileId=dest_id,
                    fields='name'
                ).execute()
                dest_name = folder.get('name')
            except Exception as e:
                print_color(f"Error getting destination folder: {str(e)}", 'red')
                return 1
        
        print_color(f"Using destination folder: {dest_name} [ID: {dest_id}]", 'green')
        
        # Confirm the operation
        print_color(f"\nReady to copy from '{source_name}' to '{dest_name}'", 'blue')
        
        if args.max_items:
            print_color(f"Will stop after processing {args.max_items} items", 'yellow')
        
        confirm = input("Continue? (y/n): ")
        if confirm.lower() != 'y':
            print_color("Operation cancelled.", 'yellow')
            return 0
        
        # Prepare progress file if needed
        progress_path = args.progress_file
        if progress_path and not args.resume:
            # Clear the progress file when starting a new run
            open(progress_path, 'w').close()
        
        # Start timer
        start_time = time.time()
        
        # Initialize file cache
        file_cache = {}
        
        # Process the folder structure
        print_color("\nStarting batch copy process...", 'blue')
        summary = process_folder_recursive(
            service, source_id, dest_id, source_name,
            is_shared_drive=False, file_cache=file_cache,
            batch_size=args.batch_size, batch_delay=args.batch_delay,
            file_delay=args.file_delay, max_items=args.max_items,
            progress_path=progress_path, resume=args.resume
        )
        
        # End timer
        elapsed_time = time.time() - start_time
        minutes, seconds = divmod(elapsed_time, 60)
        hours, minutes = divmod(minutes, 60)
        
        # Display summary results
        print_color("\n====== BATCH COPY PROCESS COMPLETED ======", 'blue')
        print(f"Time elapsed: {int(hours)}h {int(minutes)}m {int(seconds)}s")
        print(f"\nSource folder: {source_name} [ID: {source_id}]")
        print(f"Destination folder: {dest_name} [ID: {dest_id}]")
        
        print_color("\n===== OPERATION SUMMARY =====", 'blue')
        print(f"Total items processed: {summary['total_items_processed']}")
        print(f"Folders created: {summary['copied_folders']}")
        print(f"Files copied:    {summary['copied_files']} ({format_size(summary['total_copied_bytes'])})")
        print(f"Files skipped:   {summary['skipped_files']} (already exist with same size)")
        print(f"Files replaced:  {summary['replaced_files']} (existed with different size)")
        
        if summary['errors'] > 0:
            print_color(f"Errors:         {summary['errors']}", 'red')
            print_color("\nSome files or folders were not copied due to errors.", 'red')
            print("Check the output above for specific error messages.")
        
        # If max_items was specified and we hit the limit
        if args.max_items and summary['remaining_items'] <= 0:
            print_color("\nReached maximum item limit. To continue, run with: ", 'yellow')
            if progress_path:
                print_color(f"python gdrive_batch.py --source {source_id} --dest {dest_id} --progress-file {progress_path} --resume", 'yellow')
            else:
                print_color(f"python gdrive_batch.py --source {source_id} --dest {dest_id} --max-items [LIMIT]", 'yellow')
        
    except Exception as e:
        print_color(f"\n❌ Error: {str(e)}", 'red')
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())