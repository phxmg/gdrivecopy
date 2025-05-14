#!/usr/bin/env python3

import os
import pickle
import argparse
import time
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError

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
            try:
                creds.refresh(Request())
            except RefreshError:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return creds

def check_quota(verbose=False):
    """Check the Google Drive API quota and estimate when it will reset."""
    try:
        # Get credentials and build service
        creds = get_credentials()
        service = build('drive', 'v3', credentials=creds)
        
        print_color("✅ Successfully connected to Google Drive", 'green')
        print_color("\nChecking API quota status...", 'blue')
        
        # Get user info to identify the account
        user_info = service.about().get(fields="user").execute()
        display_name = user_info.get('user', {}).get('displayName', 'Unknown')
        email = user_info.get('user', {}).get('emailAddress', 'Unknown')
        
        print_color(f"Account: {display_name} ({email})", 'cyan')
        
        # Try to make a lightweight API call to check quota
        try:
            # Use a small request that counts against the quota
            files_request = service.files().list(
                pageSize=1,
                fields="files(id,name)",
                orderBy="modifiedTime desc"
            ).execute()
            
            print_color("✅ API quota available - able to make requests", 'green')
            
            # If verbose, get more information about the account storage
            if verbose:
                storage_info = service.about().get(fields="storageQuota").execute()
                storage_quota = storage_info.get('storageQuota', {})
                
                limit = int(storage_quota.get('limit', 0))
                usage = int(storage_quota.get('usage', 0))
                usage_in_drive = int(storage_quota.get('usageInDrive', 0))
                usage_in_trash = int(storage_quota.get('usageInTrash', 0))
                
                # Format storage values
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
                
                # Calculate percentage used
                percent_used = (usage / limit * 100) if limit > 0 else 0
                
                print_color("\nStorage Quota Information:", 'blue')
                print(f"Storage Limit:    {format_size(limit)}")
                print(f"Total Usage:      {format_size(usage)} ({percent_used:.1f}%)")
                print(f"Usage in Drive:   {format_size(usage_in_drive)}")
                print(f"Usage in Trash:   {format_size(usage_in_trash)}")
                
            return True, "API quota available"
            
        except Exception as e:
            # Check if this is a quota exceeded error
            if "userRateLimitExceeded" in str(e) or "User rate limit exceeded" in str(e):
                print_color("❌ API quota exceeded", 'red')
                
                # Provide information about Google Drive API quota
                print_color("\nGoogle Drive API Quota Information:", 'yellow')
                print("- Default quota is 1,000,000,000 quota units per day")
                print("- Most read operations cost 1 unit per request")
                print("- Write operations like file creation/copying cost 50-100 units")
                print("- Quotas typically reset at midnight Pacific Time (PT)")
                
                # Calculate when the quota might reset
                now = datetime.now()
                # Find when the next midnight PT is (UTC-7 or UTC-8 depending on daylight saving)
                # Simplifying, we'll assume UTC-8 for Pacific Standard Time
                pt_offset = -8  # hours
                utc_time = now + timedelta(hours=-pt_offset)  # Convert local to approx UTC
                
                # Find the next midnight PT
                next_midnight_pt = datetime(utc_time.year, utc_time.month, utc_time.day, 0, 0, 0)
                if utc_time.hour >= 0:  # If it's already past midnight PT
                    next_midnight_pt = next_midnight_pt + timedelta(days=1)
                
                # Convert back to local time
                next_reset_local = next_midnight_pt + timedelta(hours=pt_offset)
                
                # Calculate time until reset
                time_until_reset = next_reset_local - now
                hours_until_reset = time_until_reset.total_seconds() / 3600
                
                print_color(f"\nEstimated time until quota reset:", 'magenta')
                print(f"- Approximately {hours_until_reset:.1f} hours")
                print(f"- Reset expected around: {next_reset_local.strftime('%Y-%m-%d %H:%M:%S')} (your local time)")
                
                # Provide recommendations
                print_color("\nRecommendations:", 'blue')
                print("1. Wait until the quota resets (typically midnight PT)")
                print("2. Use exponential backoff in your script (already implemented)")
                print("3. Consider these options to optimize quota usage:")
                print("   - Reduce the number of API requests")
                print("   - Process files in batches with delays between batches")
                print("   - Increase retry delays when encountering rate limits")
                
                return False, f"API quota exceeded. Try again in {hours_until_reset:.1f} hours."
            else:
                print_color(f"❌ Error checking quota: {str(e)}", 'red')
                return False, f"Error: {str(e)}"
        
    except Exception as e:
        print_color(f"❌ Error connecting to Google Drive: {str(e)}", 'red')
        return False, f"Error: {str(e)}"

def test_rate_limits():
    """Test if you're still hitting rate limits by making minimal API calls."""
    print_color("\nTesting current API accessibility with minimal calls...", 'blue')
    
    try:
        # Get credentials and build service
        creds = get_credentials()
        service = build('drive', 'v3', credentials=creds)
        
        # Try a series of small API calls with increasing delays
        delays = [0, 2, 5, 10]  # seconds
        success_count = 0
        
        for delay_seconds in delays:
            if delay_seconds > 0:
                print(f"Waiting {delay_seconds} seconds before next test...")
                time.sleep(delay_seconds)
            
            try:
                print(f"Test {success_count+1}/{len(delays)}: Making minimal API call...", end=" ")
                
                # Make a minimal API call
                service.files().list(
                    pageSize=1,
                    fields="files(id)"
                ).execute()
                
                success_count += 1
                print_color("Success", 'green')
                
            except Exception as e:
                if "userRateLimitExceeded" in str(e) or "User rate limit exceeded" in str(e):
                    print_color("Failed - Rate limit exceeded", 'red')
                else:
                    print_color(f"Failed - {str(e)}", 'red')
        
        # Summarize results
        if success_count == len(delays):
            print_color(f"\n✅ All {success_count} test requests succeeded!", 'green')
            print_color("You should be able to proceed with regular API usage.", 'green')
            return True
        else:
            print_color(f"\n⚠️ {success_count}/{len(delays)} test requests succeeded", 'yellow')
            if success_count > 0:
                print_color("You are still experiencing some rate limiting, but some requests are getting through.", 'yellow')
                print_color("Consider using longer delays between requests.", 'yellow')
            else:
                print_color("You are still fully rate limited. You should wait longer before retrying.", 'red')
            return False
    
    except Exception as e:
        print_color(f"❌ Error testing rate limits: {str(e)}", 'red')
        return False

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Check Google Drive API quota status')
    parser.add_argument('--verbose', action='store_true', help='Show additional account information')
    parser.add_argument('--test', action='store_true', help='Test if you can make API calls after experiencing rate limits')
    
    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_args()
    
    print_color("=== GOOGLE DRIVE API QUOTA CHECKER ===", 'blue')
    print("This tool checks your current API quota status and recommends when to retry if rate limited.")
    
    # Check quota status
    quota_available, message = check_quota(args.verbose)
    
    # If requested, also test rate limits with a series of small API calls
    if args.test:
        success = test_rate_limits()
        
        if success and not quota_available:
            print_color("\nInteresting results: Quota check indicated problems, but test calls succeeded.", 'yellow')
            print_color("This suggests you may have limited quota available. Proceed with caution.", 'yellow')
    
    return 0 if quota_available else 1

if __name__ == "__main__":
    main()