#!/usr/bin/env python3

import os
import sys
import argparse

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Switch between Google Drive user accounts')
    parser.add_argument('user', help='Username to switch to (e.g., "val" or "turbo")')
    
    return parser.parse_args()

def main():
    args = parse_args()
    username = args.user.lower()
    
    # Check if the username is valid
    valid_users = ["val", "turbo"]
    if username not in valid_users:
        print(f"Error: '{username}' is not a valid user. Choose from: {', '.join(valid_users)}")
        return 1
    
    # Check for existing token files
    token_file = "token.pickle"
    token_exists = os.path.exists(token_file)
    token_backup = f"token.pickle.{username}"
    backup_exists = os.path.exists(token_backup)
    
    # Handle current token
    if token_exists:
        # Get the current user from any existing token
        current_user = "unknown"
        for user in valid_users:
            if os.path.exists(f"token.pickle.{user}"):
                current_user = user
                break
        
        print(f"Backing up current token as token.pickle.{current_user}")
        os.rename(token_file, f"token.pickle.{current_user}")
    
    # Set up the new user
    if backup_exists:
        print(f"Restoring token for {username} from {token_backup}")
        os.rename(token_backup, token_file)
        print(f"Successfully switched to {username}@phxmg.com")
    else:
        print(f"No existing token found for {username}@phxmg.com")
        print(f"The next time you run a script, you'll need to log in with {username}@phxmg.com")
        # Remove any existing token to force login
        if os.path.exists(token_file):
            os.remove(token_file)
            
    return 0

if __name__ == "__main__":
    sys.exit(main())