#!/usr/bin/env python3

import os
import pickle
import sys
import time
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Define the required scopes
SCOPES = ['https://www.googleapis.com/auth/drive']

def colorize(text, color_code):
    """Add color to terminal output."""
    # Check if we're in a terminal that supports colors
    try:
        import os
        if os.name == 'nt' or not sys.stdout.isatty():
            return text  # Don't use colors if not supported
        return f"\033[{color_code}m{text}\033[0m"
    except:
        return text  # If any error occurs, return plain text

def bold(text):
    """Make text bold in terminal."""
    return colorize(text, "1")

def blue(text):
    """Make text blue in terminal."""
    return colorize(text, "34")

def green(text):
    """Make text green in terminal."""
    return colorize(text, "32")

def yellow(text):
    """Make text yellow in terminal."""
    return colorize(text, "33")

def red(text):
    """Make text red in terminal."""
    return colorize(text, "31")

def print_header(text):
    """Print a formatted header."""
    try:
        width = min(os.get_terminal_size().columns, 80)
    except:
        width = 80  # Default if can't get terminal size
    
    print("\n" + "=" * width)
    print(bold(text.center(width)))
    print("=" * width + "\n")

def print_step(step_num, total_steps, description):
    """Print a formatted step indicator."""
    print(f"\n{bold(blue(f'Step {step_num}/{total_steps}:'))} {description}")

def print_item(prefix, item, indent=0):
    """Print a folder or file item with appropriate formatting."""
    is_folder = item.get('mimeType') == 'application/vnd.google-apps.folder'
    icon = "📁" if is_folder else "📄"
    
    # Calculate size string if item has size info
    size_str = ""
    if 'size' in item and not is_folder:
        size_bytes = int(item['size'])
        if size_bytes < 1024:
            size_str = f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            size_str = f"{size_bytes/1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            size_str = f"{size_bytes/(1024*1024):.1f} MB"
        else:
            size_str = f"{size_bytes/(1024*1024*1024):.1f} GB"
        size_str = f" ({size_str})"
    
    # Print indented item with ID
    indent_str = "  " * indent
    print(f"{indent_str}{prefix} {icon} {bold(item['name'])}{size_str} [ID: {item['id']}]")

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
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return creds

def list_shared_folders(service):
    """List all folders shared with the user."""
    print_step(1, 5, "Finding folders shared with you")
    
    print("Searching for folders shared with you...")
    
    try:
        # Query for shared folders and shared drives
        results = []
        page_token = None
        
        # ===== METHOD 1: Get shared folders directly marked as sharedWithMe =====
        while True:
            # sharedWithMe=true will find items shared directly with the user
            # We filter to include only folders
            response = service.files().list(
                q="sharedWithMe=true and mimeType='application/vnd.google-apps.folder'",
                spaces='drive',
                fields='nextPageToken, files(id, name, mimeType, owners, shared, sharingUser)',
                pageToken=page_token,
                pageSize=100
            ).execute()
            
            items = response.get('files', [])
            results.extend(items)
            
            page_token = response.get('nextPageToken')
            if not page_token:
                break
        
        # ===== METHOD 2: Specifically search for "Gateway Footage" folder =====
        try:
            # Try to find a folder by name "Gateway Footage"
            gateway_response = service.files().list(
                q="name='Gateway Footage' and mimeType='application/vnd.google-apps.folder'",
                spaces='drive',
                fields='files(id, name, mimeType, owners, shared, sharingUser)',
                pageSize=10
            ).execute()
            
            gateway_items = gateway_response.get('files', [])
            
            # Add any found Gateway Footage folders, avoiding duplicates
            gateway_ids = [item['id'] for item in results]
            for item in gateway_items:
                if item['id'] not in gateway_ids:
                    item['specialSearch'] = True
                    results.append(item)
        except Exception as e:
            print(yellow(f"Note: Error searching for 'Gateway Footage' folder: {str(e)}"))
        
        # ===== METHOD 3: Get shared drives and their contents =====
        try:
            # List all shared drives the user has access to
            shared_drives_response = service.drives().list(
                pageSize=50
            ).execute()
            
            shared_drives = shared_drives_response.get('drives', [])
            
            if shared_drives:
                print(f"Found {len(shared_drives)} shared drives. Checking for folders...")
                
                # First, add the shared drives themselves as virtual folders
                for drive in shared_drives:
                    drive_id = drive.get('id')
                    drive_name = drive.get('name')
                    
                    # Create a virtual folder entry for this shared drive
                    results.append({
                        'id': drive_id,
                        'name': f"{drive_name} (Shared Drive)",
                        'mimeType': 'application/vnd.google-apps.folder',
                        'isSharedDrive': True
                    })
                    
                    # Now search for "Gateway Footage" in this shared drive
                    try:
                        drive_folders = service.files().list(
                            q="name='Gateway Footage' and mimeType='application/vnd.google-apps.folder'",
                            spaces='drive',
                            fields='files(id, name, mimeType, owners, shared, sharingUser, parents)',
                            driveId=drive_id,
                            includeItemsFromAllDrives=True,
                            supportsAllDrives=True,
                            corpora='drive',
                            pageSize=10
                        ).execute()
                        
                        drive_items = drive_folders.get('files', [])
                        
                        # Add any found Gateway Footage folders
                        gateway_ids = [item['id'] for item in results]
                        for item in drive_items:
                            if item['id'] not in gateway_ids:
                                item['inSharedDrive'] = True
                                item['sharedDriveName'] = drive_name
                                item['sharedDriveId'] = drive_id
                                results.append(item)
                    except Exception as e:
                        print(yellow(f"Note: Could not search for 'Gateway Footage' in shared drive {drive_name}: {str(e)}"))
                            
        except Exception as e:
            print(yellow(f"Note: Could not access shared drives: {str(e)}"))
        
        # ===== METHOD 4: Get all accessible folders and filter for Gateway Footage =====
        try:
            # Try a broader search for shared folders by name
            broad_response = service.files().list(
                q="name contains 'Gateway' and mimeType='application/vnd.google-apps.folder'",
                spaces='drive',
                fields='files(id, name, mimeType, owners, shared, sharingUser)',
                pageSize=30
            ).execute()
            
            broad_items = broad_response.get('files', [])
            
            # Add any relevant found folders, avoiding duplicates
            existing_ids = [item['id'] for item in results]
            for item in broad_items:
                if item['id'] not in existing_ids:
                    item['broadSearch'] = True
                    results.append(item)
        except Exception as e:
            print(yellow(f"Note: Error in broad folder search: {str(e)}"))
            
        # ===== DISPLAY RESULTS =====
        if not results:
            print(yellow("\nNo shared folders found."))
            print("This might be because:")
            print("- No folders have been shared with you")
            print("- The folders are shared via a link rather than directly with your account")
            print("- The API can't access the shared folders due to permission settings")
            
            # Prompt to enter a specific folder ID if they have it
            print(yellow("\nIf you have the folder ID for 'Gateway Footage', you can enter it directly:"))
            folder_id = safe_input("Enter folder ID (or press Enter to skip): ")
            
            if folder_id and folder_id.strip():
                try:
                    # Try to get folder info
                    folder = service.files().get(
                        fileId=folder_id.strip(),
                        fields='id, name, mimeType',
                        supportsAllDrives=True
                    ).execute()
                    
                    if folder and folder.get('mimeType') == 'application/vnd.google-apps.folder':
                        folder['manualEntry'] = True
                        results.append(folder)
                        print(green(f"Successfully added folder: {folder['name']} [ID: {folder['id']}]"))
                    else:
                        print(red("The ID does not belong to a folder."))
                except Exception as e:
                    print(red(f"Error accessing folder with ID {folder_id}: {str(e)}"))
            
            if not results:
                return []
        
        print(f"\nFound {green(str(len(results)))} folders:")
        
        for i, item in enumerate(results, 1):
            # Check if it's a shared drive
            is_shared_drive = item.get('isSharedDrive', False)
            in_shared_drive = item.get('inSharedDrive', False)
            
            # Get sharing user if available and not a shared drive
            shared_by = ""
            if not is_shared_drive and 'sharingUser' in item and 'displayName' in item['sharingUser']:
                shared_by = f" [Shared by: {item['sharingUser']['displayName']}]"
            
            # Add icon based on type
            icon = "📁"
            
            # Add special markers
            special_marker = ""
            if is_shared_drive:
                shared_by = " [Shared Drive]"
            elif in_shared_drive:
                shared_by = f" [In Shared Drive: {item.get('sharedDriveName', 'Unknown')}]"
            elif item.get('specialSearch', False):
                special_marker = " 🔍"  # Special search result
            elif item.get('broadSearch', False):
                special_marker = " 🔎"  # Broad search result
            elif item.get('manualEntry', False):
                special_marker = " ✏️"  # Manually entered
                
            # Print with index for selection
            print(f"{i}. {icon} {bold(item['name'])}{special_marker}{shared_by} [ID: {item['id']}]")
        
        return results
    
    except Exception as e:
        print(red(f"Error listing shared folders: {str(e)}"))
        return []

def list_my_drive_folders(service):
    """List top-level folders in My Drive for destination selection."""
    print_step(2, 5, "Finding your folders for destination")
    
    print("Searching for folders in your My Drive...")
    
    try:
        # Query for folders in My Drive
        results = []
        page_token = None
        
        # First, get the root folder ID 'My Drive'
        root_folder = service.files().get(fileId='root', fields='id, name').execute()
        results.append(root_folder)  # Add 'My Drive' as the first option
        
        # Then get direct children folders of My Drive
        while True:
            response = service.files().list(
                q="'root' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
                spaces='drive',
                fields='nextPageToken, files(id, name, mimeType)',
                pageToken=page_token,
                pageSize=100
            ).execute()
            
            items = response.get('files', [])
            results.extend(items)
            
            page_token = response.get('nextPageToken')
            if not page_token:
                break
        
        # Display the results
        if len(results) <= 1:
            print(yellow("\nNo folders found in your My Drive."))
        
        print(f"\nFound {green(str(len(results)))} folders in your My Drive:")
        
        for i, item in enumerate(results, 1):
            # Print with index for selection
            print(f"{i}. 📁 {bold(item['name'])} [ID: {item['id']}]")
        
        return results
    
    except Exception as e:
        print(red(f"Error listing My Drive folders: {str(e)}"))
        return []

def list_folder_contents(service, folder_id, folder_name, max_depth=1, is_shared_drive=False):
    """List the contents of a folder with optional recursion."""
    print_step(3, 5, f"Previewing contents of {bold(folder_name)}")
    
    try:
        items = get_folder_contents(service, folder_id, is_shared_drive)
        
        if not items:
            print(yellow(f"\nNo items found in '{folder_name}'."))
            return
        
        print(f"\nFound {green(str(len(items)))} items in '{folder_name}':")
        
        folders = []
        files = []
        
        # Separate folders and files
        for item in items:
            if item.get('mimeType') == 'application/vnd.google-apps.folder':
                folders.append(item)
            else:
                files.append(item)
        
        # Print folders first, then files
        for i, item in enumerate(folders, 1):
            print_item(f"{i}.", item)
            
            # If we should show subfolder contents and we're not at max depth
            if max_depth > 1:
                sub_items = get_folder_contents(service, item['id'], is_shared_drive)
                for sub_item in sub_items:
                    print_item("↳", sub_item, indent=1)
        
        for i, item in enumerate(files, len(folders) + 1):
            print_item(f"{i}.", item)
            
    except Exception as e:
        print(red(f"Error listing folder contents: {str(e)}"))

# This function was replaced with the new version above that supports shared drives

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
        print(red(f"Error creating folder '{name}': {str(e)}"))
        return None

def copy_file(service, file_id, name, parent_id):
    """Copy a file to a new location."""
    file_metadata = {
        'name': name,
        'parents': [parent_id]
    }
    
    try:
        # Add supportsAllDrives to better handle files in shared drives
        # or files that are shared with limited permissions
        file = service.files().copy(
            fileId=file_id,
            body=file_metadata,
            fields='id, name',
            supportsAllDrives=True
        ).execute()
        return file
    except Exception as e:
        print(red(f"Error copying file '{name}': {str(e)}"))
        
        # Special handling for permission errors
        if "insufficientFilePermissions" in str(e):
            print(yellow(f"Permission issue detected for file '{name}'. Trying alternative approach..."))
            try:
                # If it's a permission issue, try to download and reupload the file
                # First, get the file's metadata and content
                request = service.files().get_media(fileId=file_id)
                # Create a BytesIO object to store the downloaded file
                from io import BytesIO
                file_content = BytesIO()
                downloader = MediaIoBaseDownload(file_content, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    print(f"Download progress: {int(status.progress() * 100)}%")
                
                # Reset the file pointer to the beginning of the BytesIO object
                file_content.seek(0)
                
                # Create the media body for the upload
                media = MediaIoBaseUpload(file_content, mimetype='application/octet-stream')
                
                # Create the file in the destination
                uploaded_file = service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id, name'
                ).execute()
                
                print(green(f"Successfully uploaded file '{name}' using alternative method."))
                return uploaded_file
            except Exception as inner_e:
                print(red(f"Alternative method also failed for file '{name}': {str(inner_e)}"))
                return None
        return None

def get_folder_contents(service, folder_id, is_shared_drive=False):
    """Get the contents of a folder (helper function)."""
    results = []
    page_token = None
    
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        
        # For shared drives, we need special handling
        if is_shared_drive:
            while True:
                response = service.files().list(
                    q=query,
                    spaces='drive',
                    fields='nextPageToken, files(id, name, mimeType, size)',
                    pageToken=page_token,
                    pageSize=100,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    driveId=folder_id,
                    corpora='drive'
                ).execute()
                
                items = response.get('files', [])
                results.extend(items)
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
        else:
            # Always use supportsAllDrives for all folder access
            # This helps with access to shared folders even if they are read-only
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
        print(yellow(f"Note: Some items might not be visible due to permissions. Error: {str(e)}"))
    
    return results

def copy_folder_structure(service, source_folder_id, dest_folder_id, folder_name, depth=0, is_shared_drive=False):
    """Recursively copy a folder structure."""
    # Create the folder in the destination
    print(f"{'  ' * depth}📁 Creating folder: {bold(folder_name)}")
    
    # For the root case, we either use the existing destination folder or create a new one
    if depth == 0:
        # Check if we should create a new subfolder or use the destination as is
        response = safe_input(f"Create a new subfolder '{folder_name}' in the destination? (y/n): ")
        if response.lower() == 'y':
            new_folder = create_folder(service, folder_name, dest_folder_id)
            if not new_folder:
                return False
            dest_folder_id = new_folder['id']
            print(green(f"Created new folder: {folder_name} [ID: {dest_folder_id}]"))
        else:
            print(yellow(f"Using existing destination folder [ID: {dest_folder_id}]"))
    else:
        # Always create new subfolders for nested folders
        new_folder = create_folder(service, folder_name, dest_folder_id)
        if not new_folder:
            return False
        dest_folder_id = new_folder['id']
    
    # Get all items in the source folder
    items = get_folder_contents(service, source_folder_id, is_shared_drive)
    
    # Track progress
    total_items = len(items)
    if total_items == 0:
        print(f"{'  ' * depth}📂 Folder is empty")
        return True
    
    # Count folders and files
    folder_count = sum(1 for item in items if item.get('mimeType') == 'application/vnd.google-apps.folder')
    file_count = total_items - folder_count
    print(f"{'  ' * depth}Found {green(str(total_items))} items: {folder_count} folders, {file_count} files")
    
    # Process all items in the folder
    for i, item in enumerate(items, 1):
        try:
            is_folder = item.get('mimeType') == 'application/vnd.google-apps.folder'
            
            # Progress indicator
            progress = f"[{i}/{total_items}]"
            
            if is_folder:
                # Recursively copy subfolders
                print(f"{'  ' * depth}{progress} 📂 Processing subfolder: {bold(item['name'])}")
                success = copy_folder_structure(service, item['id'], dest_folder_id, item['name'], depth + 1, is_shared_drive)
                if not success:
                    print(red(f"{'  ' * depth}⚠️ Failed to copy subfolder: {item['name']}"))
            else:
                # Copy files
                print(f"{'  ' * depth}{progress} 📄 Copying file: {bold(item['name'])}")
                file_copy = copy_file(service, item['id'], item['name'], dest_folder_id)
                if not file_copy:
                    print(red(f"{'  ' * depth}⚠️ Failed to copy file: {item['name']}"))
                else:
                    print(f"{'  ' * depth}✅ Copied file: {item['name']}")
        
        except Exception as e:
            print(red(f"{'  ' * depth}⚠️ Error processing item {item.get('name', 'unknown')}: {str(e)}"))
    
    return True

def main():
    """Main function to run the interactive Google Drive folder copy tool."""
    # Print welcome message
    print_header("Google Drive Folder Copy Tool")
    print("This tool helps you copy shared folders to your own Google Drive.")
    print("It preserves the folder structure and works with large files.")
    
    # Get credentials and build the service
    try:
        creds = get_credentials()
        service = build('drive', 'v3', credentials=creds)
        print(green("✓ Successfully connected to Google Drive"))
    except Exception as e:
        print(red(f"Error connecting to Google Drive: {str(e)}"))
        print("\nMake sure you have:")
        print("1. Installed the required packages: google-api-python-client google-auth-httplib2 google-auth-oauthlib")
        print("2. Created a Google Cloud project with the Drive API enabled")
        print("3. Downloaded the credentials.json file to the same directory as this script")
        return
    
    # Step 1: List all shared folders
    shared_folders = list_shared_folders(service)
    
    if not shared_folders:
        print(red("\nNo shared folders found. Cannot proceed."))
        return
    
    # Let user select a shared folder
    while True:
        try:
            source_choice = safe_input("\nEnter the number of the shared folder you want to copy (or 'q' to quit): ")
            
            if source_choice.lower() == 'q':
                print("Exiting program.")
                return
            
            source_index = int(source_choice) - 1
            if 0 <= source_index < len(shared_folders):
                source_folder = shared_folders[source_index]
                print(green(f"Selected: {source_folder['name']} [ID: {source_folder['id']}]"))
                break
            else:
                print(red("Invalid selection. Please try again."))
        except ValueError:
            print(red("Please enter a number or 'q' to quit."))
    
    # Step 2: List My Drive folders for destination selection
    my_folders = list_my_drive_folders(service)
    
    if not my_folders:
        print(red("\nNo destination folders found. Cannot proceed."))
        return
    
    # Let user select a destination folder
    while True:
        try:
            dest_choice = safe_input("\nEnter the number of the destination folder (or 'q' to quit): ")
            
            if dest_choice.lower() == 'q':
                print("Exiting program.")
                return
            
            dest_index = int(dest_choice) - 1
            if 0 <= dest_index < len(my_folders):
                dest_folder = my_folders[dest_index]
                print(green(f"Selected: {dest_folder['name']} [ID: {dest_folder['id']}]"))
                break
            else:
                print(red("Invalid selection. Please try again."))
        except ValueError:
            print(red("Please enter a number or 'q' to quit."))
    
    # Check if source is a shared drive
    is_shared_drive = source_folder.get('isSharedDrive', False)
    
    # Step 3: Preview the source folder contents
    list_folder_contents(service, source_folder['id'], source_folder['name'], max_depth=2, is_shared_drive=is_shared_drive)
    
    # Step 4: Preview the destination folder contents
    list_folder_contents(service, dest_folder['id'], dest_folder['name'])
    
    # Step 5: Confirm and start copying
    print_step(5, 5, "Starting the copy process")
    
    # Ask for confirmation
    confirm = safe_input(f"\nDo you want to copy '{bold(source_folder['name'])}' to '{bold(dest_folder['name'])}'? (y/n): ")
    
    if confirm.lower() != 'y':
        print(yellow("Operation cancelled."))
        return
    
    print(f"\n{bold('Starting copy process...')}")
    print("This may take a while depending on the number and size of files.")
    
    # Start timer
    start_time = time.time()
    
    # Copy the folder structure
    success = copy_folder_structure(service, source_folder['id'], dest_folder['id'], source_folder['name'], is_shared_drive=is_shared_drive)
    
    # End timer
    elapsed_time = time.time() - start_time
    minutes, seconds = divmod(elapsed_time, 60)
    hours, minutes = divmod(minutes, 60)
    
    if success:
        print_header("Copy Process Completed!")
        print(f"Time elapsed: {int(hours)}h {int(minutes)}m {int(seconds)}s")
        print(f"\nSource folder: {bold(source_folder['name'])} [ID: {source_folder['id']}]")
        print(f"Destination folder: {bold(dest_folder['name'])} [ID: {dest_folder['id']}]")
    else:
        print_header("Copy Process Completed with Errors")
        print(red("Some files or folders might not have been copied correctly."))
        print("Check the output above for specific errors.")

def safe_input(prompt):
    """Get input safely, with fallback for non-interactive environments."""
    try:
        return input(prompt)
    except (EOFError, KeyboardInterrupt):
        print("\nThis environment doesn't support interactive input.")
        print("Try running the script directly in a terminal:")
        print("  python gdrive_interactive.py")
        sys.exit(1)

if __name__ == '__main__':
    try:
        # Use the safe input function directly in functions that need it
        # Cannot override built-in input function for all modules
        main()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        sys.exit(0)
    except Exception as e:
        print(red(f"\nAn unexpected error occurred: {str(e)}"))
        sys.exit(1)