# INSTRUCTIONS:

#
#             You must first get authentication to run this code.
#             The code will only work from the notebook you authenticate on.
#             Steps:
#             1. Go to https://developers.google.com/gmail/api/quickstart/python?authuser=1
#             2. Hit 'ENABLE THE GMAIL API'
#             3. Download credentials and upload them to your working directory in your JupyterNotebook.
#                - When you first run the notebook, you will be asked to log in to a GMAIL account.
#                - You will get a saftey warning - but hit 'Advanced Options' and override the warning.


from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import base64
import pandas as pd
from httplib2 import Http
from apiclient import errors



SCOPES = ['https://mail.google.com/'] # grants all permissions. proceed with caution.

def query_for_message_ids(service, search_query):
    """searching for an e-mail (Supports the same query format as the Gmail search box.
    For example, "from:someuser@example.com rfc822msgid:<somemsgid@example.com>
    is:unread")
    """
    result = service.users().messages().list(userId='me', q=search_query).execute()
    results = result.get('messages')
    if results:
        msg_ids = [r['id'] for r in results]
    else:
        msg_ids = []

    return msg_ids


def GetAttachments(service, user_id, msg_id, prefix=""):
    """Get and store attachment from Message with given id.

    Args:
    service: Authorized Gmail API service instance.
    user_id: User's email address. The special value "me"
    can be used to indicate the authenticated user.
    msg_id: ID of Message containing attachment.
    prefix: prefix which is added to the attachment filename on saving
    """

    message = service.users().messages().get(userId=user_id, id=msg_id).execute()

    for part in message['payload']['parts']:
        if part['filename']:
            if 'data' in part['body']:
                data=part['body']['data']
            else:
                att_id=part['body']['attachmentId']
                att=service.users().messages().attachments().get(userId=user_id, messageId=msg_id,id=att_id).execute()
                data=att['data']
            file_data = base64.urlsafe_b64decode(data.encode('utf-8'))
            path = prefix+part['filename']

            print(type(file_data))
            with open(path, 'wb') as f:
                    f.write(file_data)

creds = None
# The file token.pickle stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.
if os.path.exists('token.pickle'):
    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)
# If there are no (valid) credentials available, let the user log in.
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

gmail_service = build('gmail', 'v1', credentials=creds)

# Make sure you're in the directory you want everything downloaded to before running

messageID = query_for_message_ids(gmail_service,'has:attachment')
for i in range(len(messageID)):
    GetAttachments(gmail_service, 'me', messageID[i], prefix="rtf_")
    gmail_service.users().messages().trash(userId='me', id=messageID[i]).execute()


# MOCK CODE TO MOVE TO GCS BUCKET:

#from google.cloud import storage

#def upload_to_bucket(blob_name, path_to_file, bucket_name):
#    """ Upload data to a bucket"""
#
#    # Explicitly use service account credentials by specifying the private key
#    # file.
#    storage_client = storage.Client.from_service_account_json(
#        'creds.json')

    #print(buckets = list(storage_client.list_buckets())

#    bucket = storage_client.get_bucket(bucket_name)
#    blob = bucket.blob(blob_name)
#    blob.upload_from_filename(path_to_file)

#    #returns a public url
#    return blob.public_url
