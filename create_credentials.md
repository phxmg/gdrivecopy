# Creating Google Cloud Credentials

To use the Google Drive API, you need to create a credentials file. Follow these steps:

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project
   - Click on the project dropdown at the top of the page
   - Click "New Project"
   - Enter a name (e.g., "Drive Copy")
   - Click "Create"

3. Enable the Google Drive API
   - In the left menu, go to "APIs & Services" > "Library"
   - Search for "Google Drive API"
   - Click on it and then click "Enable"

4. Configure OAuth consent screen
   - In the left menu, go to "APIs & Services" > "OAuth consent screen"
   - Select "Internal" (for G Suite accounts)
   - Click "Create"
   - Fill in the required fields (App name, User support email, Developer contact information)
   - Click "Save and Continue" 
   - Under "Scopes," click "Add or Remove Scopes"
   - Add the scope: https://www.googleapis.com/auth/drive
   - Click "Save and Continue"
   - Review and click "Back to Dashboard"

5. Create OAuth client ID
   - In the left menu, go to "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "OAuth client ID"
   - For Application type, select "Desktop application" 
   - Name it something like "Drive Copy Client"
   - Click "Create"
   - Click "Download JSON" for your client secret
   - Save the downloaded file as `credentials.json` in the same directory as the script

Now you can run the script:

```bash
python gdrive_copy.py
```

The first time you run the script, it will open a browser window asking you to authenticate. Follow the prompts to allow the application access to your Google Drive. After authentication, the script will save a token for future use.