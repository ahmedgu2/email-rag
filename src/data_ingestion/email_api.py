import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import base64
from email.utils import parseaddr, parsedate_to_datetime
from dotenv import load_dotenv
import os
from datetime import datetime

# Load .env file and fetch paths
load_dotenv()

CREDENTIALS_PATH = os.environ.get('GMAIL_CREDENTIALS_PATH')
TOKEN_PATH = os.environ.get('GMAIL_TOKEN_PATH')

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

def get_email_body(payload):
    if 'parts' in payload:
        for part in payload['parts']:
            if part['mimeType'] == 'text/plain' and 'data' in part['body']:
                return base64.urlsafe_b64decode(part['body']['data']).decode()
    elif 'body' in payload and 'data' in payload['body']:
        return base64.urlsafe_b64decode(payload['body']['data']).decode()
    return "(No plain text body found)"


def auth():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_PATH, SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(TOKEN_PATH, "w") as token:
            token.write(creds.to_json())
    return creds


def fetch_emails(creds: Credentials, from_date_time: datetime=None):
  try:
    # Call the Gmail API
    service = build("gmail", "v1", credentials=creds)

    timestamp = int(from_date_time.timestamp())
    query = f"after:{timestamp}"
    
    # Initialize variables for pagination
    page_token = None
    
    while True:
      # Get a page of messages
      results = service.users().messages().list(
          userId='me', 
          labelIds=['INBOX'], 
          q=query,
          maxResults=500,  # Max allowed by Gmail API
          pageToken=page_token
      ).execute()
      
      messages = results.get('messages', [])
      
      if not messages:
        if page_token is None:  # First page is empty
          print("No emails found.")
        break  # No more messages to process
      
      for message in messages:
        msg = service.users().messages().get(userId='me', id=message['id'], format='full').execute()

        email_id = msg.get('id')
        thread_id = msg.get('threadId')

        headers = msg['payload']['headers']
        subject = next((h['value'] for h in headers if h['name'] == 'Subject'), '(No subject)')
        sender_name, sender_email = parseaddr(next((h['value'] for h in headers if h['name'] == 'From'), '(No sender)'))
        receiver_name, receiver_email = parseaddr(next((h['value'] for h in headers if h['name'] == 'To'), '(No receiver)'))
        date_str = next((h['value'] for h in headers if h['name'] == 'Date'), None)
        date_obj = parsedate_to_datetime(date_str) if date_str else None
        body = get_email_body(msg['payload'])

        yield {
            "email_id": email_id,
            "thread_id": thread_id,
            "subject": subject,
            "sender_name": sender_name,
            "sender_email": sender_email,
            "receiver_name": receiver_name,
            "receiver_email": receiver_email,
            "date_time": date_obj,
            "body": body
        }
      
      # Get the token for the next page
      page_token = results.get('nextPageToken')
      if not page_token:
        break  # No more pages

  except HttpError as error:
    print(f"An error occurred: {error}")
